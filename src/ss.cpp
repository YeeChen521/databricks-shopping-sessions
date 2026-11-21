// Shopping Sessions: native pipeline (v2).
//
// Reads $DATA_DIR (default /data), writes $OUTPUT_DIR/{user_daily,campaign_daily}/*.parquet.
// Optional positional arguments: DATA_DIR OUTPUT_DIR.
//
// Stages
//   0. manifest, products, campaigns
//   1. links: parallel parse -> union-find -> id -> person-name table
//   2. events + clicks: parallel AVX2 parse over newline-aligned chunks of every file.
//        events are scattered by hash(event_id), clicks by hash(person)
//   3. per event partition: dedup on event_id (smallest ingest_ts), then scatter the
//        surviving (valid, not late) kept copies by hash(person)
//   4. per person partition: sort, sessionize, attribute, aggregate
//   5. Parquet output (one user_daily file per thread)
//
// IDs are packed into 16-byte keys: up to 15 bytes are stored exactly (big-endian, last byte
// is the length), so comparing keys as integers is byte-wise string order. Longer IDs get a
// 120-bit hash plus a side table holding the string (used for ordering and output).
//
// Timestamps inside the pipeline are u32 milliseconds since (window_start - 7 days); the
// window is 14 days, and anything longer than ~49 days is rejected at startup.

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <mutex>
#include <string>
#include <thread>
#include <unordered_map>
#include <vector>

#include <dirent.h>
#include <errno.h>
#include <fcntl.h>
#include <immintrin.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>

using namespace std;
#ifdef SS_GCOV
extern "C" void __gcov_dump(void);
#endif
typedef int64_t i64;
typedef uint64_t u64;
typedef uint32_t u32;
typedef int32_t i32;
typedef uint16_t u16;
typedef uint8_t u8;

static const i64 DAY = 86400000LL;
static const u32 GAP = 1800000u;
static const i64 WEEK = 7 * DAY;
static const u16 INVALID_CAT = 0xFFFF;

static int NT = 8;
static bool VERBOSE = false;
static auto T0 = chrono::steady_clock::now();
static double last_t = 0;
static void logt(const char* what) {
    if (!VERBOSE) return;
    double t = chrono::duration<double>(chrono::steady_clock::now() - T0).count();
    fprintf(stderr, "[%8.3f s] (+%7.3f) %s\n", t, t - last_t, what);
    last_t = t;
}
[[noreturn]] static void die(const string& m) {
    fprintf(stderr, "fatal: %s\n", m.c_str());
    fflush(stderr);
    _exit(1);
}

template <class F>
static void parallel_for(size_t n, F f) {
    atomic<size_t> next{0};
    auto work = [&](int tid) {
        for (;;) {
            size_t i = next.fetch_add(1, memory_order_relaxed);
            if (i >= n) break;
            f(i, tid);
        }
    };
    vector<thread> th;
    int k = (int)min<size_t>(NT, max<size_t>(n, 1));
    for (int t = 1; t < k; t++) th.emplace_back(work, t);
    work(0);
    for (auto& x : th) x.join();
}

// ---------------------------------------------------------------------------------------------
// Memory: one huge-page-backed arena, per-thread slabs, append-only block streams
// ---------------------------------------------------------------------------------------------
static const size_t HUGE = 2u << 20;
struct Arena {
    char* base = nullptr;
    size_t cap = 0;
    atomic<size_t> used{0};
    void init(size_t c) {
        c = (c + HUGE - 1) & ~(HUGE - 1);
        void* m = mmap(nullptr, c + HUGE, PROT_READ | PROT_WRITE, MAP_PRIVATE | MAP_ANONYMOUS | MAP_NORESERVE, -1, 0);
        if (m == MAP_FAILED) die("arena mmap failed");
        base = (char*)(((uintptr_t)m + HUGE - 1) & ~(uintptr_t)(HUGE - 1));
        cap = c;
        if (!getenv("NOHUGE")) madvise(base, cap, MADV_HUGEPAGE);
    }
    // zeroed memory
    char* alloc(size_t n) {
        n = (n + 63) & ~(size_t)63;
        size_t off = used.fetch_add(n, memory_order_relaxed);
        if (off + n <= cap) return base + off;
        void* m = mmap(nullptr, n, PROT_READ | PROT_WRITE, MAP_PRIVATE | MAP_ANONYMOUS | MAP_NORESERVE, -1, 0);
        if (m == MAP_FAILED) die("out of memory");
        madvise(m, n, MADV_HUGEPAGE);
        return (char*)m;
    }
};
static Arena g_arena;

static const size_t SLAB = 4u << 20;
static const size_t BLK = 8u << 10;
struct Slab {
    char* p = nullptr;
    char* e = nullptr;
    vector<char*> freeList;  // consumed blocks, reused before fresh (zeroed) memory
    char* block() {
        if (!freeList.empty()) {
            char* r = freeList.back();
            freeList.pop_back();
            return r;
        }
        if (p == e) {
            p = g_arena.alloc(SLAB);
            e = p + SLAB;
        }
        char* r = p;
        p += BLK;
        return r;
    }
    char pad[64];
};
static vector<Slab> g_slabs;  // [thread]

template <class R>
struct Stream {
    static const size_t CAP = BLK / sizeof(R);
    R* cur = nullptr;
    R* end = nullptr;
    vector<R*> blocks;
    inline void push(const R& r, Slab& s) {
        if (cur == end) {
            cur = (R*)s.block();
            end = cur + CAP;
            blocks.push_back(cur);
        }
        *cur++ = r;
    }
    size_t size() const { return blocks.empty() ? 0 : (blocks.size() - 1) * CAP + (cur - blocks.back()); }
    template <class F>
    inline void eachBlock(F f) const {
        for (size_t b = 0; b < blocks.size(); b++) {
            const R* p = blocks[b];
            f(p, (b + 1 == blocks.size()) ? (size_t)(cur - p) : CAP);
        }
    }
    void release(Slab& s) {
        for (R* b : blocks) s.freeList.push_back((char*)b);
        blocks.clear();
        blocks.shrink_to_fit();
        cur = end = nullptr;
    }
    template <class F>
    inline void each(F f) const {
        for (size_t b = 0; b < blocks.size(); b++) {
            const R* p = blocks[b];
            const R* q = (b + 1 == blocks.size()) ? cur : p + CAP;
            for (; p < q; p++) f(*p);
        }
    }
};

// ---------------------------------------------------------------------------------------------
// Keys
// ---------------------------------------------------------------------------------------------
struct Key {
    u64 a, b;
};
static inline bool operator==(Key x, Key y) { return x.a == y.a && x.b == y.b; }
static inline bool isLong(Key k) { return (k.b & 0xFF) == 0xFF; }
static inline bool isNull(Key k) { return (k.a | k.b) == 0; }

static inline u64 hk(Key k) {
    u64 x = k.a * 0x9E3779B97F4A7C15ULL ^ k.b;
    x ^= x >> 31;
    x *= 0xbf58476d1ce4e5b9ULL;
    x ^= x >> 29;
    return x;
}
static inline u64 mix64(u64 x) {
    x ^= x >> 30;
    x *= 0xbf58476d1ce4e5b9ULL;
    x ^= x >> 27;
    x *= 0x94d049bb133111ebULL;
    x ^= x >> 31;
    return x;
}
static u64 hashBytes(const char* p, size_t n, u64 seed) {
    u64 h = seed * 0x9E3779B97F4A7C15ULL ^ n;
    size_t i = 0;
    for (; i + 8 <= n; i += 8) {
        u64 w;
        memcpy(&w, p + i, 8);
        h = mix64(h ^ w) + 0x632BE59BD9B4E019ULL;
    }
    u64 w = 0;
    memcpy(&w, p + i, n - i);
    return mix64(h ^ w ^ (seed << 32));
}

struct KH {
    size_t operator()(const Key& k) const { return hk(k); }
};
static vector<vector<pair<Key, string>>> g_longs;  // [thread]
static unordered_map<Key, string, KH> g_longmap;
static void mergeLongs() {
    for (auto& v : g_longs) {
        for (auto& kv : v) g_longmap.emplace(kv.first, std::move(kv.second));
        v.clear();
        v.shrink_to_fit();
    }
}

alignas(16) static u8 MASK16[17][16];
static __m128i BSWAP_LANES;
static void initMasks() {
    for (int n = 0; n <= 16; n++)
        for (int i = 0; i < 16; i++) MASK16[n][i] = i < n ? 0xFF : 0;
    BSWAP_LANES = _mm_setr_epi8(7, 6, 5, 4, 3, 2, 1, 0, 15, 14, 13, 12, 11, 10, 9, 8);
}

// `lim` is the end of readable memory: a 16-byte load from p is safe if p + 16 <= lim.
template <bool REG>
static inline Key packKey(const char* p, size_t n, const char* lim, int tid) {
    if (n <= 15) {
        __m128i v;
        if (p + 16 <= lim) {
            v = _mm_loadu_si128((const __m128i*)p);
        } else {
            alignas(16) char buf[16] = {0};
            memcpy(buf, p, n);
            v = _mm_load_si128((const __m128i*)buf);
        }
        v = _mm_and_si128(v, _mm_load_si128((const __m128i*)MASK16[n]));
        v = _mm_insert_epi8(v, (int)n, 15);
        v = _mm_shuffle_epi8(v, BSWAP_LANES);
        return Key{(u64)_mm_extract_epi64(v, 0), (u64)_mm_extract_epi64(v, 1)};
    }
    Key k{hashBytes(p, n, 1), hashBytes(p, n, 2) | 0xFF};
    if (REG) g_longs[tid].emplace_back(k, string(p, n));
    return k;
}
static Key packStr(const string& s) { return packKey<false>(s.data(), s.size(), s.data(), 0); }

static string keyStr(Key k) {
    if (isLong(k)) {
        auto it = g_longmap.find(k);
        if (it == g_longmap.end()) die("long key missing from registry");
        return it->second;
    }
    unsigned char buf[16];
    u64 a = __builtin_bswap64(k.a), b = __builtin_bswap64(k.b);
    memcpy(buf, &a, 8);
    memcpy(buf + 8, &b, 8);
    return string((const char*)buf, buf[15]);
}

// byte-wise string order
static inline bool keyLess(Key x, Key y) {
    if (!isLong(x) && !isLong(y)) return x.a < y.a || (x.a == y.a && x.b < y.b);
    return keyStr(x) < keyStr(y);
}

// Open-addressing map Key -> V (V trivially copyable). Null key marks empty slots.
// Memory comes from the arena (already zero) unless `own` storage is used.
template <class V>
struct KeyMap {
    struct Slot {
        Key k;
        V v;
    };
    Slot* s = nullptr;
    u64 mask = 0;
    vector<Slot> own;
    static size_t slotsFor(size_t cap) {
        size_t sz = 64;
        while (sz < cap * 2) sz <<= 1;
        return sz;
    }
    void initArena(size_t cap) {
        size_t sz = slotsFor(cap);
        s = (Slot*)g_arena.alloc(sz * sizeof(Slot));
        mask = sz - 1;
    }
    void initOwn(size_t cap) {  // reusable storage, cleared
        size_t sz = slotsFor(cap);
        if (own.size() < sz) own.resize(sz);
        memset((void*)own.data(), 0, sz * sizeof(Slot));
        s = own.data();
        mask = sz - 1;
    }
    inline const V* find(Key k) const { return findH(k, hk(k)); }
    inline const V* findH(Key k, u64 h) const {
        u64 i = h & mask;
        for (;;) {
            const Slot& x = s[i];
            if (x.k == k) return &x.v;
            if (isNull(x.k)) return nullptr;
            i = (i + 1) & mask;
        }
    }
    inline void prefetchH(u64 h) const { __builtin_prefetch(&s[h & mask]); }
    inline V* insert(Key k, const V& v, bool& inserted) {
        u64 i = hk(k) & mask;
        for (;;) {
            Slot& x = s[i];
            if (x.k == k) {
                inserted = false;
                return &x.v;
            }
            if (isNull(x.k)) {
                x.k = k;
                x.v = v;
                inserted = true;
                return &x.v;
            }
            i = (i + 1) & mask;
        }
    }
};

// ---------------------------------------------------------------------------------------------
// Files
// ---------------------------------------------------------------------------------------------
struct MFile {
    const char* p = nullptr;
    size_t n = 0;
};
static MFile mapFile(const string& path) {
    MFile f;
    int fd = open(path.c_str(), O_RDONLY);
    if (fd < 0) die("cannot open " + path);
    struct stat st;
    if (fstat(fd, &st) != 0) die("cannot stat " + path);
    f.n = (size_t)st.st_size;
    if (f.n > 0) {
        void* m = mmap(nullptr, f.n, PROT_READ, MAP_PRIVATE, fd, 0);
        if (m == MAP_FAILED) die("cannot mmap " + path);
        f.p = (const char*)m;
    }
    close(fd);
    return f;
}
static vector<string> listCsv(const string& dir) {
    vector<string> out;
    DIR* d = opendir(dir.c_str());
    if (!d) return out;
    while (dirent* e = readdir(d)) {
        string name = e->d_name;
        if (name.size() < 4 || name.compare(name.size() - 4, 4, ".csv") != 0) continue;
        if (name[0] == '.') continue;
        string path = dir + "/" + name;
        struct stat st;
        if (stat(path.c_str(), &st) != 0 || !S_ISREG(st.st_mode)) continue;
        out.push_back(path);
    }
    closedir(d);
    sort(out.begin(), out.end());
    return out;
}
static string readSmall(const string& path) {
    FILE* f = fopen(path.c_str(), "rb");
    if (!f) die("cannot open " + path);
    string s;
    char buf[65536];
    size_t r;
    while ((r = fread(buf, 1, sizeof buf, f)) > 0) s.append(buf, r);
    fclose(f);
    return s;
}

// A chunk is a raw byte range of one file; it owns exactly the lines that START inside it.
// makeChunks never reads file data (no page faults on the main thread); each worker calls
// snapChunk to move s to the first line start in the range (past the header in the file's first
// chunk) and e to the end of the line containing e - 1. Adjacent chunks therefore agree.
struct Chunk {
    const char* s;
    const char* e;
    const char* lim;   // end of the mapped file
    const char* base;  // start of the mapped file
};
static void makeChunks(const vector<MFile>& files, size_t target, vector<Chunk>& out) {
    for (auto& f : files) {
        if (f.n == 0) continue;
        const char* end = f.p + f.n;
        for (const char* s = f.p; s < end; s += target) out.push_back(Chunk{s, min(end, s + target), end, f.p});
    }
}
static inline void snapChunk(Chunk& c) {
    const char* s = c.s;
    if (s == c.base || s[-1] != '\n') {
        const char* h = (const char*)memchr(s, '\n', (size_t)(c.lim - s));
        s = h ? h + 1 : c.lim;
    }
    const char* e = c.e;
    if (e < c.lim && e[-1] != '\n') {
        const char* h = (const char*)memchr(e, '\n', (size_t)(c.lim - e));
        e = h ? h + 1 : c.lim;
    }
    c.s = s;
    c.e = e < s ? s : e;
}

// ---------------------------------------------------------------------------------------------
// CSV scanning per rules.md, AVX2.
// Calls onLine(fp, fl) for each line with exactly N fields that are all well-formed; quoted
// fields are unwrapped. Malformed and empty lines are skipped.
// A field is well-formed if it holds no quote, or is "..." with exactly two quotes.
// ---------------------------------------------------------------------------------------------
static inline void blockMasks(const char* p, u64& nl, u64& cm, u64& qt) {
    __m256i a = _mm256_loadu_si256((const __m256i*)p);
    __m256i b = _mm256_loadu_si256((const __m256i*)(p + 32));
    const __m256i N = _mm256_set1_epi8('\n'), C = _mm256_set1_epi8(','), Q = _mm256_set1_epi8('"');
    nl = (u64)(u32)_mm256_movemask_epi8(_mm256_cmpeq_epi8(a, N)) |
         ((u64)(u32)_mm256_movemask_epi8(_mm256_cmpeq_epi8(b, N)) << 32);
    cm = (u64)(u32)_mm256_movemask_epi8(_mm256_cmpeq_epi8(a, C)) |
         ((u64)(u32)_mm256_movemask_epi8(_mm256_cmpeq_epi8(b, C)) << 32);
    qt = (u64)(u32)_mm256_movemask_epi8(_mm256_cmpeq_epi8(a, Q)) |
         ((u64)(u32)_mm256_movemask_epi8(_mm256_cmpeq_epi8(b, Q)) << 32);
}

template <int N, class F>
static inline void scanLines(const char* s, const char* e, F&& onLine) {
    const char* fp[N];
    u32 fl[N];
    int nf = 0;
    bool bad = false;
    const char* fstart = s;
    const char* lineStart = s;
    u64 qAtStart = 0, qBase = 0;
    auto field = [&](const char* t, u64 qcnt) {
        if (bad) return;
        if (nf >= N) {
            bad = true;
            return;
        }
        u32 len = (u32)(t - fstart);
        u64 q = qcnt - qAtStart;
        if (q == 0) {
            fp[nf] = fstart;
            fl[nf] = len;
        } else if (q == 2 && len >= 2 && fstart[0] == '"' && t[-1] == '"') {
            fp[nf] = fstart + 1;
            fl[nf] = len - 2;
        } else {
            bad = true;
            return;
        }
        nf++;
    };
    for (const char* blk = s; blk < e; blk += 64) {
        u64 nl, cm, qt;
        if (blk + 64 <= e) {
            blockMasks(blk, nl, cm, qt);
        } else {
            alignas(32) char tmp[64];
            memset(tmp, 0, 64);
            memcpy(tmp, blk, e - blk);
            blockMasks(tmp, nl, cm, qt);
        }
        u64 term = nl | cm;
        while (term) {
            int pos = __builtin_ctzll(term);
            term &= term - 1;
            const char* t = blk + pos;
            u64 qcnt = qBase + (u64)__builtin_popcountll(qt & ((1ULL << pos) - 1));
            field(t, qcnt);
            if ((nl >> pos) & 1) {
                if (!bad && nf == N) onLine(fp, fl);
                nf = 0;
                bad = false;
                lineStart = t + 1;
            }
            fstart = t + 1;
            qAtStart = qcnt;
        }
        qBase += (u64)__builtin_popcountll(qt);
    }
    if (lineStart < e) {  // last line without '\n'
        field(e, qBase);
        if (!bad && nf == N) onLine(fp, fl);
    }
}

// Scalar version for small lookup files.
template <int N>
static inline bool splitLine(const char* s, const char* e, const char** fp, u32* fl) {
    if (s == e) return false;
    int i = 0;
    const char* p = s;
    for (;;) {
        if (i == N) return false;
        if (p < e && *p == '"') {
            const char* q = p + 1;
            while (q < e && *q != '"' && *q != ',') q++;
            if (q == e || *q == ',') return false;
            fp[i] = p + 1;
            fl[i] = (u32)(q - p - 1);
            p = q + 1;
            if (p < e && *p != ',') return false;
        } else {
            const char* q = p;
            while (q < e && *q != ',') {
                if (*q == '"') return false;
                q++;
            }
            fp[i] = p;
            fl[i] = (u32)(q - p);
            p = q;
        }
        i++;
        if (p == e) break;
        p++;
    }
    return i == N;
}

// Line-at-a-time scanner (same contract as scanLines). For a line of at most 126 bytes, the
// 128 bytes at its start give comma / quote / newline bitmasks; the field count is one popcount
// and quoting is validated for the whole line with a few 128-bit operations:
//   field starts S = (C << 1) | 1, field ends E = T >> 1 with terminators T = C | bit(L);
//   start quotes Qs = Q & S, end quotes Qe = Q & E. Valid iff every quote is a start or an end
//   quote, no quote is both (a lone `"` field), and each start quote's field ends with a quote:
//   adding Qs to the non-terminator bits M carries from a field's start to its terminator, so
//   ((M + Qs) & T) >> 1 is exactly the set of required end quotes.
// Longer lines use the scalar splitter.
typedef unsigned __int128 u128;
static inline int ctz128(u128 x) {
    u64 lo = (u64)x;
    return lo ? __builtin_ctzll(lo) : 64 + __builtin_ctzll((u64)(x >> 64));
}
static inline int popc128(u128 x) { return __builtin_popcountll((u64)x) + __builtin_popcountll((u64)(x >> 64)); }

template <int N, class F>
static inline void scanLines2(const char* s, const char* e, const char* lim, F&& onLine) {
    const char* fp[N];
    u32 fl[N];
    alignas(64) char tmp[128];
    while (s < e) {
        const char* base = s;
        if (s + 128 > lim) {
            memset(tmp, 0, 128);
            memcpy(tmp, s, (size_t)(lim - s));
            base = tmp;
        }
        u64 nl0, cm0, qt0, nl1, cm1, qt1;
        blockMasks(base, nl0, cm0, qt0);
        blockMasks(base + 64, nl1, cm1, qt1);
        u128 NL = (u128)nl0 | ((u128)nl1 << 64);
        size_t rem = (size_t)(e - s);
        if (rem < 128) NL &= ((u128)1 << rem) - 1;
        u32 L;
        const char* next;
        if (NL) {
            L = (u32)ctz128(NL);
            next = s + L + 1;
        } else if (rem < 128) {
            L = (u32)rem;  // last line, no '\n'
            next = e;
        } else {
            L = 1000;  // long line
            next = nullptr;
        }
        if (L > 126) {
            const char* nlp = (const char*)memchr(s, '\n', rem);
            const char* le = nlp ? nlp : e;
            if (splitLine<N>(s, le, fp, fl)) onLine(fp, fl);
            s = nlp ? nlp + 1 : e;
            continue;
        }
        const char* ls = s;
        s = next;
        if (L == 0) continue;  // empty line
        u128 lm = ((u128)1 << L) - 1;
        u128 C = ((u128)cm0 | ((u128)cm1 << 64)) & lm;
        if (popc128(C) != N - 1) continue;
        u128 Q = ((u128)qt0 | ((u128)qt1 << 64)) & lm;
        u128 Qs = 0;
        if (Q) {
            u128 T = C | ((u128)1 << L);
            Qs = Q & ((C << 1) | 1);
            u128 Qe = Q & (T >> 1);
            if ((Q & ~(Qs | Qe)) | (Qs & Qe)) continue;
            u128 M = ~T & (((u128)1 << (L + 1)) - 1);
            if (Qe != (((M + Qs) & T) >> 1)) continue;
        }
        u32 a = 0;
        for (int i = 0; i < N; i++) {
            u32 b;
            if (i < N - 1) {
                b = (u32)ctz128(C);
                C &= C - 1;
            } else {
                b = L;
            }
            u32 q = (u32)(Qs >> a) & 1;
            fp[i] = ls + a + q;
            fl[i] = b - a - 2 * q;
            a = b + 1;
        }
        onLine(fp, fl);
    }
}

// ---------------------------------------------------------------------------------------------
// Timestamps
// ---------------------------------------------------------------------------------------------
static inline i64 daysFromCivil(i64 y, i64 m, i64 d) {
    y -= m <= 2;
    i64 era = (y >= 0 ? y : y - 399) / 400;
    i64 yoe = y - era * 400;
    i64 doy = (153 * ((m + 9) % 12) + 2) / 5 + d - 1;
    i64 doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    return era * 146097 + doe - 719468;
}
static inline bool dig2(const char* p, int& v) {
    unsigned a = (unsigned char)p[0] - '0', b = (unsigned char)p[1] - '0';
    if (a > 9 || b > 9) return false;
    v = (int)(a * 10 + b);
    return true;
}
static const int MDAYS[13] = {0, 31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31};

// SWAR: per byte, (x ^ '0') is 0..9 exactly for ASCII digits. `bad` has 0x80 set in every byte
// of `mask` that is not a digit (the 0x7F mask keeps the +0x76 from carrying across bytes).
static inline u64 nonDigits(u64 x, u64 mask) {
    u64 t = x ^ 0x3030303030303030ULL;
    return (((t & 0x7F7F7F7F7F7F7F7FULL) + 0x7676767676767676ULL) | t) & 0x8080808080808080ULL & mask;
}
static inline u64 ld8(const char* p) {
    u64 v;
    memcpy(&v, p, 8);
    return v;
}
static inline int dgt(u64 t, int byte) { return (int)((t >> (8 * byte)) & 0xFF); }

// F1/F2/F3 -> epoch ms. false = bad timestamp. F3 values beyond int64 map to INT64_MAX
// (outside every window).
static inline bool parseTs(const char* p, u32 n, i64& out) {
    if (n == 0) return false;
    if (n >= 20 && p[4] == '-') {
        if (n != 20 && n != 25) return false;
        // "YYYY-MM-" "DDTHH:MM" ":SS" + ("Z" | "+HH:MM")
        u64 A = ld8(p), B = ld8(p + 8);
        u32 C;
        memcpy(&C, p + 16, 4);
        const u64 AM = 0x00FFFF00FFFFFFFFULL, BM = 0xFFFF00FFFF00FFFFULL;
        if (nonDigits(A, AM) | nonDigits(B, BM) | nonDigits((u64)C, 0x0000000000FFFF00ULL)) return false;
        if ((A & ~AM) != 0x2D00002D00000000ULL || (B & ~BM) != 0x00003A0000540000ULL || (C & 0xFF) != ':')
            return false;
        u64 ta = A ^ 0x3030303030303030ULL, tb = B ^ 0x3030303030303030ULL;
        u32 tc = C ^ 0x30303030u;
        int h = dgt(tb, 3) * 10 + dgt(tb, 4);
        int mi = dgt(tb, 6) * 10 + dgt(tb, 7);
        int se = (int)((tc >> 8) & 0xFF) * 10 + (int)((tc >> 16) & 0xFF);
        if (h > 23 || mi > 59 || se > 59) return false;
        // date part "YYYY-MM-DD": a few distinct values per dataset, so validated days are cached
        struct DateCache {
            u64 a;
            u32 d2;
            i32 days;
        };
        static thread_local DateCache dc[64];
        u32 d2 = (u32)(B & 0xFFFF);
        DateCache& c = dc[(mix64(A ^ ((u64)d2 << 48)) >> 58)];
        i64 days;
        if (c.a == A && c.d2 == d2 && c.d2 != 0) {
            days = c.days;
        } else {
            int y = dgt(ta, 0) * 1000 + dgt(ta, 1) * 100 + dgt(ta, 2) * 10 + dgt(ta, 3);
            int mo = dgt(ta, 5) * 10 + dgt(ta, 6);
            int d = dgt(tb, 0) * 10 + dgt(tb, 1);
            if (mo < 1 || mo > 12 || d < 1) return false;
            int md = MDAYS[mo];
            if (mo == 2 && (y % 4 == 0 && (y % 100 != 0 || y % 400 == 0))) md = 29;
            if (d > md) return false;
            days = daysFromCivil(y, mo, d);
            c = DateCache{A, d2, (i32)days};
        }
        i64 t = days * DAY + (i64)h * 3600000 + (i64)mi * 60000 + (i64)se * 1000;
        char z = p[19];
        if (n == 20) {
            if (z != 'Z') return false;
        } else {
            int oh, om;
            if ((z != '+' && z != '-') || !dig2(p + 20, oh) || p[22] != ':' || !dig2(p + 23, om)) return false;
            if (oh > 23 || om > 59) return false;
            i64 off = ((i64)oh * 60 + om) * 60000;
            t -= z == '+' ? off : -off;
        }
        out = t;
        return true;
    }
    if (n >= 9 && n <= 16) {
        // SWAR: last 8 digits + up to 8 leading ones; the first digit sits in the lowest byte
        u64 lo = ld8(p + n - 8), hi = ld8(p) << (8 * (16 - n));  // hi keeps only the first n-8 bytes
        hi |= n == 16 ? 0 : (0x3030303030303030ULL >> (8 * (n - 8)));  // missing leading digits = '0'
        if (nonDigits(lo, ~0ull) | nonDigits(hi, ~0ull)) return false;
        auto conv8 = [](u64 v) {
            v ^= 0x3030303030303030ULL;
            v = (v * 10 + (v >> 8)) & 0x00FF00FF00FF00FFULL;
            v = (v * 100 + (v >> 16)) & 0x0000FFFF0000FFFFULL;
            return (v * 10000 + (v >> 32)) & 0xFFFFFFFFULL;
        };
        u64 hv = conv8(hi);
        out = (i64)(hv * 100000000ULL + conv8(lo));
        return true;
    }
    u32 i = 0;
    while (i < n && p[i] == '0') i++;
    for (u32 j = i; j < n; j++)
        if ((unsigned)((unsigned char)p[j] - '0') > 9) return false;
    if (n - i > 19) {
        out = INT64_MAX;
        return true;
    }
    u64 v = 0;
    for (u32 j = i; j < n; j++) v = v * 10 + (unsigned)(p[j] - '0');
    out = v > (u64)INT64_MAX ? INT64_MAX : (i64)v;
    return true;
}

static inline bool parsePrice(const char* p, u32 n, u32& out) {
    if (n == 0) return false;
    u32 i = 0;
    while (i < n && p[i] == '0') i++;
    for (u32 j = i; j < n; j++)
        if ((unsigned)((unsigned char)p[j] - '0') > 9) return false;
    u32 nd = n - i;
    if (nd == 0 || nd > 8) return false;
    u32 v = 0;
    for (u32 j = i; j < n; j++) v = v * 10 + (unsigned)(p[j] - '0');
    if (v > 10000000) return false;
    out = v;
    return true;
}

// ---------------------------------------------------------------------------------------------
// Globals
// ---------------------------------------------------------------------------------------------
static string DATA, OUT;
static i64 WS, WE, CWS;  // window start/end, click window start (epoch ms)
static u32 EV_LO, EV_HI;  // event window, relative to CWS: [EV_LO, EV_HI)
static u32 CL_HI;         // click window, relative: [0, CL_HI)
static i64 DAY0;          // floor(WS / DAY)
static int NDAYS;

static KeyMap<u16> g_products;
// Product IDs of the form p<digits> (no leading zero, value < 2^20) are looked up in a small
// direct array instead of the hash table; every other ID uses the table. Exact either way.
static vector<u16> g_catByNum;  // number -> category; NOCAT = not in products.csv
static const u16 NOCAT = 0xFFFE;
static inline int productNum(const char* p, u32 n) {
    if (n < 2 || n > 7 || p[0] != 'p' || p[1] < '1' || p[1] > '9') return -1;
    int v = 0;
    for (u32 i = 1; i < n; i++) {
        unsigned d = (unsigned char)p[i] - '0';
        if (d > 9) return -1;
        v = v * 10 + (int)d;
    }
    return v;  // < 10^6 < 2^20
}
static vector<string> g_catNames;
static vector<int> g_catRank;
static int g_unknownCat;
static KeyMap<u32> g_campaigns;
static vector<string> g_campNames, g_campChannels;
static int NCAMP;  // index NCAMP = organic

// ---------------------------------------------------------------------------------------------
// Global ID table: every user ID (from links, events, clicks) gets a slot. The person ID (pid)
// of an ID is the slot index of its union-find root: for linked IDs the component's root slot,
// for any other ID its own slot. Keys are the compact 8-byte IDs (see cid64); lock-free insert
// with a 64-bit CAS on the key.
// ---------------------------------------------------------------------------------------------
struct alignas(16) IdSlot {
    u64 k;    // 0 = empty
    u32 val;  // 0 = not yet published; otherwise pid + 1 (link phase: 1 = marker)
    u32 pad;
};
static IdSlot* T = nullptr;
static u64 TSIZE = 0;
static inline u64 tslot(u64 h) { return (u64)(((unsigned __int128)h * TSIZE) >> 64); }  // h -> [0, TSIZE)
static const int MAXPROBE = 2048;
static atomic<bool> g_tableFull{false};
static vector<u64> g_inserted;  // [thread * 8]
static u64* g_names = nullptr;  // pid -> person name for linked persons (0 otherwise)

static inline u32 waitVal(IdSlot* s) {
    u32 v;
    while ((v = __atomic_load_n(&s->val, __ATOMIC_ACQUIRE)) == 0) _mm_pause();
    return v;
}
// Returns the slot index; *pv receives val (pid + 1). LINK: new keys get the marker 1.
template <bool LINK>
static inline u32 idInsert(u64 k, u64 h, int tid, u32* pv) {
    u64 i = tslot(h);
    for (int probe = 0; probe < MAXPROBE;) {
        IdSlot* s = &T[i];
        u64 a = __atomic_load_n(&s->k, __ATOMIC_ACQUIRE);
        if (a == k) {
            *pv = waitVal(s);
            return (u32)i;
        }
        if (a == 0) {
            u64 expected = 0;
            if (__atomic_compare_exchange_n(&s->k, &expected, k, false, __ATOMIC_ACQ_REL, __ATOMIC_ACQUIRE)) {
                u32 val = LINK ? 1u : (u32)i + 1;
                __atomic_store_n(&s->val, val, __ATOMIC_RELEASE);
                g_inserted[tid * 8]++;
                *pv = val;
                return (u32)i;
            }
            continue;  // lost the race for this slot: look at it again
        }
        if (++i == TSIZE) i = 0;
        probe++;
    }
    g_tableFull.store(true, memory_order_relaxed);
    *pv = 1;
    return 0;
}
static inline void idPrefetch(u64 h) { __builtin_prefetch(&T[tslot(h)]); }
static inline u64 personName(u32 pid) {
    u64 n = g_names[pid];
    return n ? n : T[pid].k;
}

static int PB = 9;  // partition bits
static int NP = 1 << 9;
static int QB = 7;  // person partition bits (coarser than the event partitions)
static int NQ = 1 << 7;
static inline int partOf(u64 h) { return (int)(h >> (64 - PB)); }

// ---------------------------------------------------------------------------------------------
// Compact 64-bit IDs for event_id / click_id. Up to 9 characters from [-0-9A-Z_a-z] are packed
// 7 bits each, left-aligned, with codes in byte order and 0 as padding: exact, and integer order
// is byte-wise string order. Any other ID gets an exact number from a side table (top bit set);
// comparing those falls back to the strings.
// ---------------------------------------------------------------------------------------------
static u8 CCODE[256];   // byte -> 1..64, 0 = not in the alphabet
static char CCHAR[65];  // code -> byte
static void initCompact() {
    const char* alpha = "-0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ_abcdefghijklmnopqrstuvwxyz";  // byte order
    for (int i = 0; i < 64; i++) {
        CCODE[(u8)alpha[i]] = (u8)(i + 1);
        CCHAR[i + 1] = alpha[i];
    }
}
static const u64 SIDE_BIT = 1ull << 63;
static mutex g_sideMu;
static unordered_map<string, u64> g_sideId;
static vector<string> g_sideStr;
static u64 sideId(const char* p, size_t n) {
    lock_guard<mutex> g(g_sideMu);
    auto it = g_sideId.emplace(string(p, n), SIDE_BIT | g_sideStr.size());
    if (it.second) g_sideStr.emplace_back(p, n);
    return it.first->second;
}
static inline u64 cid64(const char* p, size_t n) {  // scalar reference / fallback
    if (n <= 9) {
        u64 v = 0;
        size_t i = 0;
        for (; i < n; i++) {
            u8 c = CCODE[(u8)p[i]];
            if (!c) break;
            v = (v << 7) | c;
        }
        if (i == n) return v << (7 * (9 - n));
    }
    return sideId(p, n);
}
// SIMD version of cid64: byte ranges -> 7-bit codes, one validity check, bswap + pext to pack.
// Needs 16 readable bytes at p (p + 16 <= lim); otherwise uses the scalar version.
static inline u64 cid64f(const char* p, size_t n, const char* lim) {
    if (n > 9 || p + 16 > lim) return cid64(p, n);
    __m128i v = _mm_loadu_si128((const __m128i*)p);
    // signed byte compares: bytes >= 0x80 are negative and fall outside every range
    auto inRange = [&](char lo, char hi) {
        return _mm_and_si128(_mm_cmpgt_epi8(v, _mm_set1_epi8((char)(lo - 1))), _mm_cmplt_epi8(v, _mm_set1_epi8((char)(hi + 1))));
    };
    __m128i dig = inRange('0', '9'), up = inRange('A', 'Z'), low = inRange('a', 'z');
    __m128i dash = _mm_cmpeq_epi8(v, _mm_set1_epi8('-')), und = _mm_cmpeq_epi8(v, _mm_set1_epi8('_'));
    // code = byte - offset: '-'->1, '0'..'9'->2..11, 'A'..'Z'->12..37, '_'->38, 'a'..'z'->39..64
    __m128i off = _mm_or_si128(
        _mm_or_si128(_mm_and_si128(dig, _mm_set1_epi8(0x2E)), _mm_and_si128(up, _mm_set1_epi8(0x35))),
        _mm_or_si128(_mm_or_si128(_mm_and_si128(low, _mm_set1_epi8(0x3A)), _mm_and_si128(dash, _mm_set1_epi8(0x2C))),
                     _mm_and_si128(und, _mm_set1_epi8(0x5F - 38))));
    __m128i ok = _mm_or_si128(_mm_or_si128(dig, up), _mm_or_si128(low, _mm_or_si128(dash, und)));
    u32 lenMask = (1u << n) - 1;
    if (((u32)_mm_movemask_epi8(ok) & lenMask) != lenMask) return sideId(p, n);
    __m128i code = _mm_and_si128(_mm_sub_epi8(v, off), _mm_load_si128((const __m128i*)MASK16[n]));
    u64 lo = (u64)_mm_cvtsi128_si64(code);                   // codes 0..7 (code 0 in the lowest byte)
    u64 c8 = (u64)_mm_extract_epi8(code, 8);                  // code 8
    return (_pext_u64(__builtin_bswap64(lo), 0x7F7F7F7F7F7F7F7FULL) << 7) | c8;
}
static string cidStr(u64 v) {
    if (v & SIDE_BIT) {
        lock_guard<mutex> g(g_sideMu);
        return g_sideStr[v & ~SIDE_BIT];
    }
    string s;
    for (int i = 8; i >= 0; i--) {
        u8 c = (u8)((v >> (7 * i)) & 127);
        if (!c) break;
        s.push_back(CCHAR[c]);
    }
    return s;
}
static inline bool cidLess(u64 a, u64 b) {
    if (!((a | b) & SIDE_BIT)) return a < b;
    return cidStr(a) < cidStr(b);
}

#pragma pack(push, 4)
struct ERec {  // event copy, partitioned by event_id
    u64 eid;
    u32 pid;
    u32 ts;
    i32 ing;  // seconds relative to CWS
    u32 price;
    u16 cat;  // INVALID_CAT: kept copy would be dropped
    u16 pad;
};
struct SRec {  // surviving event, partitioned by pid
    u32 pid;
    u32 ts;
    u32 price;
    u32 cat;
};
struct CRec {  // valid click, partitioned by pid
    u64 cid;
    u32 pid;
    u32 ts;
    u32 camp;
};
#pragma pack(pop)
static_assert(sizeof(ERec) == 28, "ERec");
static_assert(sizeof(SRec) == 16, "SRec");
static_assert(sizeof(CRec) == 20, "CRec");

// ---------------------------------------------------------------------------------------------
// Lookups
// ---------------------------------------------------------------------------------------------
template <class F>
static void forEachLine(const string& text, F f) {
    const char* s = text.data();
    const char* end = s + text.size();
    const char* h = (const char*)memchr(s, '\n', text.size());
    if (!h) return;
    s = h + 1;
    while (s < end) {
        const char* nl = (const char*)memchr(s, '\n', end - s);
        const char* e = nl ? nl : end;
        f(s, e);
        s = nl ? nl + 1 : end;
    }
}

static i64 fdiv(i64 a, i64 b) { return a >= 0 ? a / b : -((-a + b - 1) / b); }

static void loadManifest() {
    string m = readSmall(DATA + "/manifest.json");
    auto get = [&](const char* name) -> i64 {
        size_t k = m.find(string("\"") + name + "\"");
        if (k == string::npos) die(string("manifest missing ") + name);
        size_t c = m.find(':', k);
        size_t q1 = m.find('"', c + 1);
        size_t q2 = m.find('"', q1 + 1);
        if (c == string::npos || q1 == string::npos || q2 == string::npos) die("bad manifest");
        string v = m.substr(q1 + 1, q2 - q1 - 1);
        i64 t;
        if (!parseTs(v.data(), (u32)v.size(), t)) die("bad manifest timestamp " + v);
        return t;
    };
    WS = get("window_start");
    WE = get("window_end");
    CWS = WS - WEEK;
    if (WE <= WS) die("empty window");
    if (WE - CWS >= (i64)UINT32_MAX) die("window too long for this build");
    EV_LO = (u32)(WS - CWS);
    EV_HI = (u32)(WE - CWS);
    CL_HI = EV_HI;
    DAY0 = fdiv(WS, DAY);
    NDAYS = (int)(fdiv(WE - 1, DAY) - DAY0 + 1);
}

static void loadLookups() {
    string pt = readSmall(DATA + "/products.csv");
    unordered_map<string, int> catIdx;
    vector<pair<Key, u16>> prods;
    forEachLine(pt, [&](const char* s, const char* e) {
        const char* fp[2];
        u32 fl[2];
        if (!splitLine<2>(s, e, fp, fl) || fl[0] == 0) return;
        string c(fp[1], fl[1]);
        auto it = catIdx.find(c);
        int ci;
        if (it == catIdx.end()) {
            ci = (int)g_catNames.size();
            catIdx.emplace(c, ci);
            g_catNames.push_back(c);
        } else
            ci = it->second;
        prods.emplace_back(packStr(string(fp[0], fl[0])), (u16)ci);
    });
    auto it = catIdx.find("unknown");
    if (it == catIdx.end()) {
        g_unknownCat = (int)g_catNames.size();
        g_catNames.push_back("unknown");
    } else
        g_unknownCat = it->second;
    if (g_catNames.size() > 60000) die("too many categories");
    g_products.initArena(prods.size() + 1);
    int maxNum = -1;
    vector<pair<int, u16>> direct;
    for (auto& pr : prods) {
        string id = keyStr(pr.first);
        int num = productNum(id.data(), (u32)id.size());
        if (num >= 0) {
            direct.emplace_back(num, pr.second);
            maxNum = max(maxNum, num);
        } else {
            bool ins;
            g_products.insert(pr.first, pr.second, ins);
        }
    }
    g_catByNum.assign((size_t)(maxNum + 1), NOCAT);
    for (auto& d : direct) g_catByNum[d.first] = d.second;
    vector<int> order(g_catNames.size());
    for (size_t i = 0; i < order.size(); i++) order[i] = (int)i;
    sort(order.begin(), order.end(), [](int a, int b) { return g_catNames[a] < g_catNames[b]; });
    g_catRank.assign(order.size(), 0);
    for (size_t r = 0; r < order.size(); r++) g_catRank[order[r]] = (int)r;

    string ct = readSmall(DATA + "/campaigns.csv");
    vector<pair<Key, u32>> camps;
    forEachLine(ct, [&](const char* s, const char* e) {
        const char* fp[2];
        u32 fl[2];
        if (!splitLine<2>(s, e, fp, fl) || fl[0] == 0) return;
        camps.emplace_back(packStr(string(fp[0], fl[0])), (u32)g_campNames.size());
        g_campNames.emplace_back(fp[0], fl[0]);
        g_campChannels.emplace_back(fp[1], fl[1]);
    });
    NCAMP = (int)g_campNames.size();
    g_campaigns.initArena(camps.size() + 1);
    for (auto& c : camps) {
        bool ins;
        g_campaigns.insert(c.first, c.second, ins);
    }
}

// ---------------------------------------------------------------------------------------------
// Links
// ---------------------------------------------------------------------------------------------
static vector<Chunk> g_linkChunks;

static vector<vector<u64>> g_linkPairs;  // [thread] key pairs, flattened
static size_t parseLinks() {
    auto& pairs = g_linkPairs;
    pairs.assign(NT, {});
    parallel_for(g_linkChunks.size(), [&](size_t ci, int tid) {
        auto& out = pairs[tid];
        Chunk c = g_linkChunks[ci];
        snapChunk(c);
        scanLines2<2>(c.s, c.e, c.lim, [&](const char** fp, const u32* fl) {
            if (fl[0] && fl[1]) {
                out.push_back(cid64f(fp[0], fl[0], c.lim));
                out.push_back(cid64f(fp[1], fl[1], c.lim));
            }
        });
    });
    mergeLongs();
    size_t total = 0;
    for (auto& v : pairs) total += v.size();
    return total;
}

static void buildLinks() {
    auto& pairs = g_linkPairs;
    size_t total = 0;
    for (auto& v : pairs) total += v.size();
    if (total == 0) return;

    // node = slot index
    vector<vector<u32>> slots(NT);
    parallel_for(NT, [&](size_t t, int tid) {
        auto& v = pairs[t];
        auto& out = slots[t];
        out.resize(v.size());
        const size_t B = 16;
        u64 hs[B];
        for (size_t base = 0; base < v.size(); base += B) {
            size_t lim = min(v.size(), base + B);
            for (size_t i = base; i < lim; i++) {
                hs[i - base] = mix64(v[i]);
                idPrefetch(hs[i - base]);
            }
            for (size_t i = base; i < lim; i++) {
                u32 pv;
                out[i] = idInsert<true>(v[i], hs[i - base], tid, &pv);
            }
        }
    });
    if (g_tableFull) return;
    logt("links intern");
    // Lock-free union-find. uf[x] = parent + 1, 0 = root (arena memory starts zeroed).
    // A root is only ever linked under a smaller root (CAS), so parent < child always, and
    // path halving may store any ancestor: all ancestor relations are permanent.
    u32* uf = (u32*)g_arena.alloc(TSIZE * sizeof(u32));
    auto find = [&](u32 x) {
        for (;;) {
            u32 p1 = __atomic_load_n(&uf[x], __ATOMIC_RELAXED);
            if (!p1) return x;
            u32 g1 = __atomic_load_n(&uf[p1 - 1], __ATOMIC_RELAXED);
            if (!g1) return p1 - 1;
            __atomic_store_n(&uf[x], g1, __ATOMIC_RELAXED);
            x = g1 - 1;
        }
    };
    parallel_for(NT, [&](size_t t, int) {
        auto& v = slots[t];
        for (size_t i = 0; i < v.size(); i += 2) {
            u32 a = v[i], b = v[i + 1];
            for (;;) {
                a = find(a);
                b = find(b);
                if (a == b) break;
                if (a < b) swap(a, b);
                if (__sync_bool_compare_and_swap(&uf[a], 0u, b + 1)) break;
            }
        }
    });
    logt("links uf");
    // pid of a linked slot = its root; person name = smallest member key (atomic min per root)
    size_t NB = (TSIZE + 65535) / 65536;
    parallel_for(NB, [&](size_t blk, int) {
        u64 lo = blk * 65536, hi = min<u64>(TSIZE, lo + 65536);
        for (u64 i = lo; i < hi; i++) {
            IdSlot& s = T[i];
            if (s.val == 0) continue;
            u32 r = (u32)i;
            while (uf[r]) r = uf[r] - 1;  // read-only walk (other threads read uf too)
            s.val = r + 1;
            u64 k = s.k;
            u64 cur = __atomic_load_n(&g_names[r], __ATOMIC_RELAXED);
            while ((cur == 0 || cidLess(k, cur)) &&
                   !__atomic_compare_exchange_n(&g_names[r], &cur, k, false, __ATOMIC_RELAXED, __ATOMIC_RELAXED)) {
            }
        }
    });
}

// ---------------------------------------------------------------------------------------------
// Events and clicks parsing
// ---------------------------------------------------------------------------------------------
static vector<vector<Stream<ERec>>> evBuf;  // [thread][event partition]
static vector<vector<Stream<CRec>>> clBuf;  // [thread][person partition]

static const size_t RING = 16;  // lookup prefetch distance, in lines

struct EvItem {
    ERec r;
    u64 uid;
    Key pid;
    u64 hu, hp;
    int pcat;  // >= 0: category already known (direct product lookup); -1: use the table
};

static void parseEventChunk(Chunk c, int tid) {
    snapChunk(c);
    auto& out = evBuf[tid];
    Slab& slab = g_slabs[tid];
    const char* lim = c.lim;
    // ring: the line parsed now prefetches its lookups; the line RING lines older is resolved
    EvItem items[RING];
    size_t nIn = 0, nOut = 0;
    auto resolve = [&](EvItem& it) {
        if (it.r.cat != INVALID_CAT) {
            u32 pv;
            idInsert<false>(it.uid, it.hu, tid, &pv);
            it.r.pid = pv - 1;
            if (it.pcat >= 0) {
                it.r.cat = (u16)it.pcat;
            } else {
                const u16* cat = g_products.findH(it.pid, it.hp);
                it.r.cat = cat ? *cat : (u16)g_unknownCat;
            }
        }
        out[partOf(mix64(it.r.eid))].push(it.r, slab);
    };
    scanLines2<7>(c.s, c.e, c.lim, [&](const char** fp, const u32* fl) {
        if (fl[0] == 0) return;
        if (nIn - nOut == RING) resolve(items[nOut++ % RING]);
        EvItem& it = items[nIn++ % RING];
        ERec& r = it.r;
        r.eid = cid64f(fp[0], fl[0], lim);
        i64 ing;
        if (parseTs(fp[3], fl[3], ing)) {
            i64 s = fdiv(ing, 1000) - CWS / 1000;
            r.ing = s > INT32_MAX ? INT32_MAX : s < INT32_MIN ? INT32_MIN : (i32)s;
        } else {
            ing = INT64_MAX;
            r.ing = INT32_MAX;
        }
        r.cat = INVALID_CAT;
        r.pid = 0;
        r.ts = 0;
        r.price = 0;
        r.pad = 0;
        i64 ts;
        if (fl[1] != 0 && fl[5] != 0 && parseTs(fp[2], fl[2], ts) && ts >= WS && ts < WE) {
            int typ = -1;
            if (fl[4] == 4) {
                if (memcmp(fp[4], "view", 4) == 0 || memcmp(fp[4], "cart", 4) == 0) typ = 0;
            } else if (fl[4] == 8 && memcmp(fp[4], "purchase", 8) == 0)
                typ = 1;
            u32 price = 0;
            if (typ >= 0 && (typ == 0 || parsePrice(fp[6], fl[6], price)) && !(ing - ts > DAY)) {
                r.cat = 0;  // valid; resolved in flush
                r.ts = (u32)(ts - CWS);
                r.price = price;
                it.uid = cid64f(fp[1], fl[1], lim);
                it.hu = mix64(it.uid);
                idPrefetch(it.hu);
                int num = productNum(fp[5], fl[5]);
                if (num >= 0) {
                    u16 c = (size_t)num < g_catByNum.size() ? g_catByNum[num] : NOCAT;
                    it.pcat = c == NOCAT ? g_unknownCat : c;
                } else {
                    it.pcat = -1;
                    it.pid = packKey<false>(fp[5], fl[5], lim, tid);
                    it.hp = hk(it.pid);
                    g_products.prefetchH(it.hp);
                }
            }
        }
    });
    while (nOut < nIn) resolve(items[nOut++ % RING]);
}

struct ClItem {
    CRec r;
    u64 uid;
    u64 hu;
};

static void parseClickChunk(Chunk c, int tid) {
    snapChunk(c);
    auto& out = clBuf[tid];
    Slab& slab = g_slabs[tid];
    const char* lim = c.lim;
    ClItem items[RING];
    size_t nIn = 0, nOut = 0;
    const u32 pmask = (u32)NQ - 1;
    auto resolve = [&](ClItem& it) {
        u32 pv;
        idInsert<false>(it.uid, it.hu, tid, &pv);
        it.r.pid = pv - 1;
        out[it.r.pid & pmask].push(it.r, slab);
    };
    scanLines2<4>(c.s, c.e, c.lim, [&](const char** fp, const u32* fl) {
        if (fl[0] == 0 || fl[1] == 0 || fl[2] == 0) return;
        i64 ts;
        if (!parseTs(fp[3], fl[3], ts) || ts < CWS || ts >= WE) return;
        const u32* camp = g_campaigns.find(packKey<false>(fp[2], fl[2], lim, tid));
        if (!camp) return;
        if (nIn - nOut == RING) resolve(items[nOut++ % RING]);
        ClItem& it = items[nIn++ % RING];
        it.uid = cid64f(fp[1], fl[1], lim);
        it.r.cid = cid64f(fp[0], fl[0], lim);
        it.r.ts = (u32)(ts - CWS);
        it.r.camp = *camp;
        it.hu = mix64(it.uid);
        idPrefetch(it.hu);
    });
    while (nOut < nIn) resolve(items[nOut++ % RING]);
}

// ---------------------------------------------------------------------------------------------
// Dedup
// ---------------------------------------------------------------------------------------------
static vector<vector<Stream<SRec>>> sBuf;  // [thread][person partition]
// The table holds the current winner's payload, so records are read once and the output is a
// sequential scan of the table.
#pragma pack(push, 4)
struct DedupSlot {  // k == 0: empty (a compact ID is never 0)
    u64 k;
    i32 ing;
    u32 pid;
    u32 ts;
    u32 price;
    u16 cat;
    u16 pad;
};
#pragma pack(pop)
static_assert(sizeof(DedupSlot) == 28, "DedupSlot");
static vector<vector<DedupSlot>> g_dedupTab;  // [thread]

static void dedupPartition(int p, int tid) {
    size_t n = 0;
    for (int t = 0; t < NT; t++) n += evBuf[t][p].size();
    if (n == 0) return;
    size_t sz = 64;
    while (sz < n + n / 2 + n / 8) sz <<= 1;  // load factor <= ~0.6
    auto& tabv = g_dedupTab[tid];
    if (tabv.size() < sz) tabv.resize(sz);
    DedupSlot* tab = tabv.data();
    memset((void*)tab, 0, sz * sizeof(DedupSlot));
    u64 mask = sz - 1;
    const int PF = 8;
    u64 hs[Stream<ERec>::CAP];
    for (int t = 0; t < NT; t++) {
        evBuf[t][p].eachBlock([&](const ERec* rs, size_t cnt) {
            for (size_t j = 0; j < cnt; j++) hs[j] = mix64(rs[j].eid);
            for (size_t j = 0; j < cnt; j++) {
                if (j + PF < cnt) __builtin_prefetch(&tab[hs[j + PF] & mask]);
                const ERec& r = rs[j];
                u64 i = hs[j] & mask;
                for (;;) {
                    DedupSlot& x = tab[i];
                    if (x.k == 0) {
                        x = DedupSlot{r.eid, r.ing, r.pid, r.ts, r.price, r.cat, 0};
                        break;
                    }
                    if (x.k == r.eid) {
                        if (r.ing < x.ing) x = DedupSlot{r.eid, r.ing, r.pid, r.ts, r.price, r.cat, 0};
                        break;
                    }
                    i = (i + 1) & mask;
                }
            }
        });
    }
    Slab& slab = g_slabs[tid];
    for (int t = 0; t < NT; t++) evBuf[t][p].release(slab);
    auto& out = sBuf[tid];
    const u32 pmask = (u32)NQ - 1;
    for (size_t i = 0; i < sz; i++) {
        const DedupSlot& x = tab[i];
        if (x.cat != INVALID_CAT && x.k != 0) out[x.pid & pmask].push(SRec{x.pid, x.ts, x.price, x.cat}, slab);
    }
}

// ---------------------------------------------------------------------------------------------
// Sessions and attribution
// ---------------------------------------------------------------------------------------------
struct URow {
    u32 pid;
    i32 day;
    u32 sessions;
    u32 events;
    i64 revenue;
    i32 topcat;  // -1 = null
    u32 pad;
};
struct CampAgg {  // credited sessions per (campaign, day), per thread
    u32 sessions, purch;
    i64 revenue;
};
static vector<vector<URow>> uRows;
static vector<vector<CampAgg>> campAgg;
static vector<vector<u32>> campAssist;  // assisted sessions per (campaign, day), per thread (dense: stays in L2)

struct PEv {
    u32 ts;
    u32 price;
    u32 cat;
};
struct PCl {
    u32 ts;
    u32 camp;
    u64 cid;
};

// LSD radix sort by ts (11 + 11 + 10 bits), stable; for persons with many events.
static void radixSortTs(PEv* a, size_t n, vector<PEv>& tmp) {
    if (tmp.size() < n) tmp.resize(n);
    PEv* src = a;
    PEv* dst = tmp.data();
    for (int pass = 0; pass < 3; pass++) {
        int shift = pass * 11;
        u32 c[2048];
        memset(c, 0, sizeof c);
        for (size_t i = 0; i < n; i++) c[(src[i].ts >> shift) & 2047]++;
        if (c[(src[0].ts >> shift) & 2047] == n) continue;  // all in one bucket
        u32 sum = 0;
        for (int b = 0; b < 2048; b++) {
            u32 x = c[b];
            c[b] = sum;
            sum += x;
        }
        for (size_t i = 0; i < n; i++) dst[c[(src[i].ts >> shift) & 2047]++] = src[i];
        swap(src, dst);
    }
    if (src != a) memcpy((void*)a, src, n * sizeof(PEv));
}

struct Scratch {
    vector<PEv> tmp;
    vector<i64> catRev;
    vector<int> catTouched;
    vector<u32> campCnt, campStart;
    vector<u32> cnt, ccnt;
    vector<PEv> ev;
    vector<PCl> cl;
    vector<u32> sStart, sDayEnd;
    vector<i32> sDay;
    vector<i64> sRev;
};
static vector<Scratch> scratch;

static void personPartition(int q, int tid) {
    size_t ns = 0, nc = 0;
    for (int t = 0; t < NT; t++) {
        ns += sBuf[t][q].size();
        nc += clBuf[t][q].size();
    }
    if (ns == 0) return;
    Scratch& sc = scratch[tid];
    const size_t L = (TSIZE >> QB) + 1;  // local index of a pid = pid >> QB
    // counting sort of events by local index; cnt[k] = start of local k
    auto& cnt = sc.cnt;
    cnt.assign(L + 1, 0);
    for (int t = 0; t < NT; t++) sBuf[t][q].each([&](const SRec& r) { cnt[(r.pid >> QB) + 1]++; });
    for (size_t k = 0; k < L; k++) cnt[k + 1] += cnt[k];
    auto& ev = sc.ev;
    if (ev.size() < ns) ev.resize(ns);
    for (int t = 0; t < NT; t++)
        sBuf[t][q].each([&](const SRec& r) { ev[cnt[r.pid >> QB]++] = PEv{r.ts, r.price, r.cat}; });
    for (size_t k = L; k > 0; k--) cnt[k] = cnt[k - 1];
    cnt[0] = 0;
    auto& ccnt = sc.ccnt;
    ccnt.assign(L + 1, 0);
    auto& cl = sc.cl;
    if (nc) {
        for (int t = 0; t < NT; t++) clBuf[t][q].each([&](const CRec& r) { ccnt[(r.pid >> QB) + 1]++; });
        for (size_t k = 0; k < L; k++) ccnt[k + 1] += ccnt[k];
        if (cl.size() < nc) cl.resize(nc);
        for (int t = 0; t < NT; t++)
            clBuf[t][q].each([&](const CRec& r) { cl[ccnt[r.pid >> QB]++] = PCl{r.ts, r.camp, r.cid}; });
        for (size_t k = L; k > 0; k--) ccnt[k] = ccnt[k - 1];
        ccnt[0] = 0;
    }
    auto& rows = uRows[tid];
    auto& agg = campAgg[tid];
    auto& assist = campAssist[tid];
    auto& catRev = sc.catRev;
    auto& touched = sc.catTouched;
    auto& campCnt = sc.campCnt;
    auto& campStart = sc.campStart;
    auto& sStart = sc.sStart;
    auto& sDay = sc.sDay;
    auto& sRev = sc.sRev;
    auto& sDayEnd = sc.sDayEnd;

    for (size_t k = 0; k < L; k++) {
        size_t n = cnt[k + 1] - cnt[k];
        if (n == 0) continue;
        u32 pid = (u32)((k << QB) | (size_t)q);
        PEv* e = ev.data() + cnt[k];
        if (n > 4096) {
            radixSortTs(e, n, sc.tmp);
        } else if (n <= 32) {
            for (size_t i = 1; i < n; i++) {
                PEv x = e[i];
                size_t j = i;
                while (j > 0 && e[j - 1].ts > x.ts) {
                    e[j] = e[j - 1];
                    j--;
                }
                e[j] = x;
            }
        } else {
            sort(e, e + n, [](const PEv& a, const PEv& b) { return a.ts < b.ts; });
        }

        // sessions + user_daily rows
        sStart.clear();
        sDay.clear();
        sRev.clear();
        int curDay = INT32_MIN;
        URow row{};
        auto flushDay = [&]() {
            if (curDay == INT32_MIN) return;
            int best = -1;
            i64 bestRev = -1;
            for (int ci : touched) {
                i64 v = catRev[ci];
                if (v > bestRev || (v == bestRev && g_catRank[ci] < g_catRank[best])) {
                    best = ci;
                    bestRev = v;
                }
                catRev[ci] = 0;
            }
            touched.clear();
            row.topcat = best;
            rows.push_back(row);
        };
        size_t i = 0;
        while (i < n) {
            u32 start = e[i].ts;
            u32 events = 0;
            i64 rev = 0;
            u32 prev = start;
            int day = (int)fdiv(CWS + start, DAY);
            if (day != curDay) {
                flushDay();
                curDay = day;
                row = URow{pid, day, 0, 0, 0, -1, 0};
            }
            while (i < n && e[i].ts - prev <= GAP) {
                prev = e[i].ts;
                events++;
                if (e[i].price) {
                    rev += e[i].price;
                    u32 ci = e[i].cat;
                    if (catRev[ci] == 0) touched.push_back((int)ci);
                    catRev[ci] += e[i].price;
                }
                i++;
            }
            row.sessions++;
            row.events += events;
            row.revenue += rev;
            sStart.push_back(start);
            sDay.push_back(day - (int)DAY0);
            sRev.push_back(rev);
        }
        flushDay();
        size_t S = sStart.size();

        // attribution
        size_t m = ccnt[k + 1] - ccnt[k];
        if (m == 0) {
            for (size_t s = 0; s < S; s++) {
                CampAgg& a = agg[(size_t)NCAMP * NDAYS + sDay[s]];
                a.sessions++;
                a.revenue += sRev[s];
                if (sRev[s] > 0) a.purch++;
            }
            continue;
        }
        PCl* c = cl.data() + ccnt[k];
        if (m > 1) {
            sort(c, c + m, [](const PCl& a, const PCl& b) {
                if (a.ts != b.ts) return a.ts < b.ts;
                return cidLess(b.cid, a.cid);  // click_id descending: last = smallest
            });
        }
        sDayEnd.resize(S);
        sDayEnd[S - 1] = (u32)S;
        for (size_t s = S - 1; s-- > 0;) sDayEnd[s] = sDay[s] == sDay[s + 1] ? sDayEnd[s + 1] : (u32)(s + 1);
        // a campaign assists every session in [campStart[cp], index where it deactivates)
        auto addInterval = [&](u32 cp, size_t a, size_t b) {
            while (a < b) {
                size_t e2 = min<size_t>(b, sDayEnd[a]);
                assist[(size_t)cp * NDAYS + sDay[a]] += (u32)(e2 - a);
                a = e2;
            }
        };
        size_t lo = 0, hi = 0;
        for (size_t s = 0; s < S; s++) {
            u32 start = sStart[s];
            while (hi < m && c[hi].ts <= start) {
                u32 cp = c[hi].camp;
                if (campCnt[cp]++ == 0) campStart[cp] = (u32)s;
                hi++;
            }
            u32 from = start - (u32)WEEK;  // start >= WEEK for events in the window
            while (lo < hi && c[lo].ts < from) {
                u32 cp = c[lo].camp;
                if (--campCnt[cp] == 0) addInterval(cp, campStart[cp], s);
                lo++;
            }
            u32 credited = hi > lo ? c[hi - 1].camp : (u32)NCAMP;
            CampAgg& a = agg[(size_t)credited * NDAYS + sDay[s]];
            a.sessions++;
            a.revenue += sRev[s];
            if (sRev[s] > 0) a.purch++;
        }
        for (; lo < hi; lo++) {
            u32 cp = c[lo].camp;
            if (campCnt[cp]) {
                addInterval(cp, campStart[cp], S);
                campCnt[cp] = 0;
            }
        }
    }
}

// ---------------------------------------------------------------------------------------------
// Parquet writer (uncompressed, PLAIN, one data page per column chunk)
// ---------------------------------------------------------------------------------------------
struct TW {  // thrift compact protocol
    string b;
    vector<int> lastId{0};
    void varint(u64 v) {
        while (v >= 0x80) {
            b.push_back((char)(v | 0x80));
            v >>= 7;
        }
        b.push_back((char)v);
    }
    static u64 zz(i64 v) { return ((u64)v << 1) ^ (u64)(v >> 63); }
    void fieldHeader(int id, int type) {
        int delta = id - lastId.back();
        if (delta > 0 && delta <= 15)
            b.push_back((char)((delta << 4) | type));
        else {
            b.push_back((char)type);
            varint(zz(id));
        }
        lastId.back() = id;
    }
    void i32f(int id, i64 v) {
        fieldHeader(id, 5);
        varint(zz(v));
    }
    void i64f(int id, i64 v) {
        fieldHeader(id, 6);
        varint(zz(v));
    }
    void strf(int id, const string& s) {
        fieldHeader(id, 8);
        varint(s.size());
        b += s;
    }
    void structBegin(int id) {
        fieldHeader(id, 12);
        lastId.push_back(0);
    }
    void structEnd() {
        b.push_back(0);
        lastId.pop_back();
    }
    void listBegin(int id, int elemType, size_t n) {
        fieldHeader(id, 9);
        if (n < 15)
            b.push_back((char)((n << 4) | elemType));
        else {
            b.push_back((char)(0xF0 | elemType));
            varint(n);
        }
    }
    void elemBegin() { lastId.push_back(0); }
    void elemEnd() {
        b.push_back(0);
        lastId.pop_back();
    }
    void listI32(i64 v) { varint(zz(v)); }
    void listStr(const string& s) {
        varint(s.size());
        b += s;
    }
};

enum PType { P_INT32 = 1, P_INT64 = 2, P_BYTES = 6 };
struct ColSpec {
    const char* name;
    int ptype;
    bool optional;
    int logical;  // 0 none, 1 string, 2 date
};

static void writeAll(int fd, const char* p, size_t n, const string& path) {
    while (n) {
        ssize_t w = write(fd, p, n);
        if (w <= 0) die("write failed " + path);
        p += w;
        n -= (size_t)w;
    }
}

// pages[ci] = encoded page body (levels + values) for column ci
static void writeParquet(const string& path, const vector<ColSpec>& cols, vector<string>& pages, i64 numRows) {
    int fd = open(path.c_str(), O_WRONLY | O_CREAT | O_TRUNC, 0644);
    if (fd < 0) die("cannot write " + path);
    i64 offset = 4;
    writeAll(fd, "PAR1", 4, path);
    vector<i64> offs, sizes;
    if (numRows > 0) {
        for (size_t ci = 0; ci < cols.size(); ci++) {
            TW ph;
            ph.i32f(1, 0);  // DATA_PAGE
            ph.i32f(2, (i64)pages[ci].size());
            ph.i32f(3, (i64)pages[ci].size());
            ph.structBegin(5);
            ph.i32f(1, numRows);
            ph.i32f(2, 0);  // PLAIN
            ph.i32f(3, 3);  // RLE
            ph.i32f(4, 3);  // RLE
            ph.structEnd();
            ph.b.push_back(0);
            writeAll(fd, ph.b.data(), ph.b.size(), path);
            writeAll(fd, pages[ci].data(), pages[ci].size(), path);
            offs.push_back(offset);
            sizes.push_back((i64)(ph.b.size() + pages[ci].size()));
            offset += sizes.back();
        }
    }
    TW fm;
    fm.i32f(1, 1);
    fm.listBegin(2, 12, cols.size() + 1);
    fm.elemBegin();
    fm.strf(4, "schema");
    fm.i32f(5, (i64)cols.size());
    fm.elemEnd();
    for (auto& c : cols) {
        fm.elemBegin();
        fm.i32f(1, c.ptype);
        fm.i32f(3, c.optional ? 1 : 0);
        fm.strf(4, c.name);
        if (c.logical == 1) fm.i32f(6, 0);  // UTF8
        if (c.logical == 2) fm.i32f(6, 6);  // DATE
        if (c.logical) {
            fm.structBegin(10);
            fm.structBegin(c.logical == 1 ? 1 : 6);
            fm.structEnd();
            fm.structEnd();
        }
        fm.elemEnd();
    }
    fm.i64f(3, numRows);
    fm.listBegin(4, 12, numRows > 0 ? 1 : 0);
    if (numRows > 0) {
        fm.elemBegin();
        fm.listBegin(1, 12, cols.size());
        i64 total = 0;
        for (size_t ci = 0; ci < cols.size(); ci++) {
            auto& c = cols[ci];
            fm.elemBegin();
            fm.i64f(2, offs[ci]);
            fm.structBegin(3);
            fm.i32f(1, c.ptype);
            fm.listBegin(2, 5, 2);
            fm.listI32(0);
            fm.listI32(3);
            fm.listBegin(3, 8, 1);
            fm.listStr(c.name);
            fm.i32f(4, 0);
            fm.i64f(5, numRows);
            fm.i64f(6, sizes[ci]);
            fm.i64f(7, sizes[ci]);
            fm.i64f(9, offs[ci]);
            fm.structEnd();
            fm.elemEnd();
            total += sizes[ci];
        }
        fm.i64f(2, total);
        fm.i64f(3, numRows);
        fm.elemEnd();
    }
    fm.strf(6, "shopping-sessions-native");
    fm.b.push_back(0);
    u32 len = (u32)fm.b.size();
    fm.b.append((const char*)&len, 4);
    fm.b += "PAR1";
    writeAll(fd, fm.b.data(), fm.b.size(), path);
    close(fd);
}

struct Buf {
    string s;
    inline void i32(int32_t v) { s.append((const char*)&v, 4); }
    inline void i64v(i64 v) { s.append((const char*)&v, 8); }
    inline void str(const char* p, u32 n) {
        s.append((const char*)&n, 4);
        s.append(p, n);
    }
    inline void str(const string& v) { str(v.data(), (u32)v.size()); }
};
static string defLevels(const vector<u8>& d) {
    size_t groups = (d.size() + 7) / 8;
    string enc;
    u64 h = ((u64)groups << 1) | 1;
    while (h >= 0x80) {
        enc.push_back((char)(h | 0x80));
        h >>= 7;
    }
    enc.push_back((char)h);
    size_t base = enc.size();
    enc.resize(base + groups, 0);
    for (size_t i = 0; i < d.size(); i++)
        if (d[i]) enc[base + i / 8] |= (char)(1 << (i % 8));
    u32 len = (u32)enc.size();
    return string((const char*)&len, 4) + enc;
}

static void writeUserFile(const string& path, const vector<URow>& rows) {
    vector<ColSpec> cols = {{"user_id", P_BYTES, false, 1},      {"day", P_INT32, false, 2},
                            {"sessions", P_INT32, false, 0},     {"events", P_INT32, false, 0},
                            {"revenue_cents", P_INT64, false, 0}, {"top_category", P_BYTES, true, 1}};
    size_t n = rows.size();
    Buf pu, pd, ps, pe, pr, pt;
    pu.s.reserve(n * 14);
    pd.s.reserve(n * 4);
    ps.s.reserve(n * 4);
    pe.s.reserve(n * 4);
    pr.s.reserve(n * 8);
    pt.s.reserve(n * 4);
    vector<u8> def(n);
    for (size_t i = 0; i < n; i++) {
        const URow& r = rows[i];
        u64 person = personName(r.pid);
        if (!(person & SIDE_BIT)) {
            char buf[9];
            int len = 0;
            for (int j = 8; j >= 0; j--) {
                u8 c = (u8)((person >> (7 * j)) & 127);
                if (!c) break;
                buf[len++] = CCHAR[c];
            }
            pu.str(buf, (u32)len);
        } else {
            pu.str(cidStr(person));
        }
        pd.i32(r.day);
        ps.i32((int32_t)r.sessions);
        pe.i32((int32_t)r.events);
        pr.i64v(r.revenue);
        if (r.topcat >= 0) {
            def[i] = 1;
            pt.str(g_catNames[r.topcat]);
        }
    }
    vector<string> pages(6);
    pages[0] = std::move(pu.s);
    pages[1] = std::move(pd.s);
    pages[2] = std::move(ps.s);
    pages[3] = std::move(pe.s);
    pages[4] = std::move(pr.s);
    pages[5] = defLevels(def) + pt.s;
    writeParquet(path, cols, pages, (i64)n);
}

static void writeCampaignFile(const string& path) {
    vector<ColSpec> cols = {{"campaign_id", P_BYTES, false, 1},       {"channel", P_BYTES, false, 1},
                            {"day", P_INT32, false, 2},               {"sessions", P_INT64, false, 0},
                            {"purchasing_sessions", P_INT64, false, 0}, {"revenue_cents", P_INT64, false, 0},
                            {"assisted_sessions", P_INT64, false, 0}};
    Buf pc, ph, pd, ps, pp, pr, pa;
    i64 n = 0;
    for (int cp = 0; cp <= NCAMP; cp++) {
        for (int di = 0; di < NDAYS; di++) {
            struct {
                i64 sessions, purch, revenue, assisted;
            } a{0, 0, 0, 0};
            for (int t = 0; t < NT; t++) {
                const CampAgg& x = campAgg[t][(size_t)cp * NDAYS + di];
                a.sessions += x.sessions;
                a.purch += x.purch;
                a.revenue += x.revenue;
                a.assisted += campAssist[t][(size_t)cp * NDAYS + di];
            }
            if (a.sessions == 0 && a.assisted == 0) continue;
            pc.str(cp == NCAMP ? string("organic") : g_campNames[cp]);
            ph.str(cp == NCAMP ? string("organic") : g_campChannels[cp]);
            pd.i32((int32_t)(DAY0 + di));
            ps.i64v(a.sessions);
            pp.i64v(a.purch);
            pr.i64v(a.revenue);
            pa.i64v(cp == NCAMP ? 0 : a.assisted);
            n++;
        }
    }
    vector<string> pages = {std::move(pc.s), std::move(ph.s), std::move(pd.s), std::move(ps.s),
                            std::move(pp.s), std::move(pr.s), std::move(pa.s)};
    writeParquet(path, cols, pages, n);
}

static void mkdirs(const string& p) {
    if (mkdir(p.c_str(), 0777) != 0 && errno != EEXIST) die("mkdir " + p);
}

// ---------------------------------------------------------------------------------------------
#if !defined(SS_TSTEST) && !defined(SS_SCANTEST) && !defined(SS_BENCH) && !defined(SS_CIDTEST)
int main(int argc, char** argv) {
    const char* v;
    DATA = (v = getenv("DATA_DIR")) ? v : "/data";
    OUT = (v = getenv("OUTPUT_DIR")) ? v : "/output";
    if (argc >= 3) {
        DATA = argv[1];
        OUT = argv[2];
    }
    VERBOSE = (v = getenv("VERBOSE")) && v[0] == '1';
    unsigned hc = thread::hardware_concurrency();
    NT = (v = getenv("NCPU")) ? atoi(v) : (hc ? (int)hc : 8);
    if ((v = getenv("THREADS"))) NT = atoi(v);
    if (NT < 1) NT = 1;
    if (NT > 64) NT = 64;
    int tscale = (v = getenv("SS_TSCALE")) ? atoi(v) : 0;  // set when re-executed with a bigger table
    g_longs.assign(NT, {});
    g_slabs.assign(NT, Slab{});
    g_inserted.assign((size_t)NT * 8, 0);
    initMasks();
    initCompact();

    // inputs
    vector<string> evPaths = listCsv(DATA + "/events"), clPaths = listCsv(DATA + "/clicks"),
                   lkPaths = listCsv(DATA + "/links");
    vector<MFile> evFiles, clFiles, lkFiles;
    for (auto& p : evPaths) evFiles.push_back(mapFile(p));
    for (auto& p : clPaths) clFiles.push_back(mapFile(p));
    for (auto& p : lkPaths) lkFiles.push_back(mapFile(p));
    size_t evBytes = 0, clBytes = 0, lkBytes = 0;
    for (auto& f : evFiles) evBytes += f.n;
    for (auto& f : clFiles) clBytes += f.n;
    for (auto& f : lkFiles) lkBytes += f.n;
    // partitions: ~20k events each, so a partition's dedup table fits in L2
    PB = 8;
    while (PB < 13 && (evBytes / 80) >> PB > 20000) PB++;
    NP = 1 << PB;
    QB = PB > 2 ? PB - 2 : PB;
    NQ = 1 << QB;
    g_arena.init(evBytes * 2 + clBytes * 2 + lkBytes * 64 + ((size_t)NT * NP * 3 * BLK) + (2ull << 30));
    auto checkTable = [&]() {
        u64 ins = 0;
        for (int t = 0; t < NT; t++) ins += g_inserted[t * 8];
        if (g_tableFull || ins > TSIZE / 10 * 8) {
            if (tscale >= 6) die("ID table overflow");
            string sc = to_string(tscale + 2);
            setenv("SS_TSCALE", sc.c_str(), 1);
            if (VERBOSE) fprintf(stderr, "ID table too small (%llu ids); re-executing with scale %s\n", (unsigned long long)ins, sc.c_str());
            execv("/proc/self/exe", argv);
            die("re-exec failed");
        }
    };

    // Chunks are computed without touching file data. A background thread asks the kernel to read
    // every chunk ahead, in processing order (links, events, clicks), so a cold page cache is
    // filled with large asynchronous reads while the lookups and links are being built.
    size_t chunkLk = 256 << 10, chunkEv = 2 << 20;
    if ((v = getenv("SS_CHUNK"))) chunkLk = chunkEv = (size_t)max(1, atoi(v));  // test hook: tiny chunks
    makeChunks(lkFiles, chunkLk, g_linkChunks);
    vector<Chunk> evChunks, clChunks;
    makeChunks(evFiles, chunkEv, evChunks);
    makeChunks(clFiles, chunkEv, clChunks);
    thread readahead([&]() {
        for (auto* cs : {&g_linkChunks, &evChunks, &clChunks})
            for (auto& c : *cs) {
                uintptr_t a = (uintptr_t)c.s & ~(uintptr_t)4095;
                madvise((void*)a, (uintptr_t)c.e - a, MADV_WILLNEED);
            }
    });
    loadManifest();
    loadLookups();
    logt("lookups");
    size_t linkKeys = parseLinks();
    logt("links parse");
    // ID table: distinct IDs ~ 60% of link key occurrences + ~1.5% of event rows (the generator's
    // single-ID people). Sized for load <= ~2/3; a re-exec with a bigger table covers misses.
    double est = 0.7 * (double)linkKeys + 0.02 * ((double)evBytes / 80.0) + 0.005 * ((double)clBytes / 45.0);
    // Any size works (slot = multiply-high of the hash), so the load is set directly: ~0.55.
    TSIZE = max<u64>(1u << 16, (u64)(est * 1.5)) << tscale;
    if ((v = getenv("SS_TSHRINK"))) TSIZE = max<u64>(64, TSIZE >> atoi(v));  // test hook: undersize
    if (TSIZE > (1ull << 31)) die("ID table too large");
    T = (IdSlot*)g_arena.alloc(TSIZE * sizeof(IdSlot));
    g_names = (u64*)g_arena.alloc(TSIZE * sizeof(u64));
    buildLinks();
    checkTable();
    logt("links");

    evBuf.assign(NT, vector<Stream<ERec>>(NP));
    clBuf.assign(NT, vector<Stream<CRec>>(NQ));
    sBuf.assign(NT, vector<Stream<SRec>>(NQ));
    logt("chunks");

    size_t nE = evChunks.size();
    vector<Chunk> all(evChunks);
    all.insert(all.end(), clChunks.begin(), clChunks.end());
    bool populate = !getenv("SS_NOPOPULATE");
    parallel_for(all.size(), [&](size_t i, int tid) {
        if (populate) {
            // map this chunk's page-cache pages in one call instead of one fault per 4 KB page
            uintptr_t a = (uintptr_t)all[i].s & ~(uintptr_t)4095;
            madvise((void*)a, (uintptr_t)all[i].e - a, 22 /* MADV_POPULATE_READ */);
        }
        if (i < nE)
            parseEventChunk(all[i], tid);
        else
            parseClickChunk(all[i], tid);
    });
    checkTable();
    mergeLongs();
    readahead.join();
    for (auto* fs : {&evFiles, &clFiles, &lkFiles})
        for (auto& f : *fs)
            if (f.n) munmap((void*)f.p, f.n);
    logt("parse events+clicks");

    g_dedupTab.assign(NT, {});
    parallel_for(NP, [&](size_t p, int tid) { dedupPartition((int)p, tid); });
    logt("dedup");

    uRows.assign(NT, {});
    campAgg.assign(NT, vector<CampAgg>((size_t)(NCAMP + 1) * NDAYS, CampAgg{0, 0, 0}));
    campAssist.assign(NT, vector<u32>((size_t)(NCAMP + 1) * NDAYS, 0));
    scratch.resize(NT);
    for (auto& s : scratch) {
        s.catRev.assign(g_catNames.size(), 0);
        s.campCnt.assign(NCAMP + 1, 0);
        s.campStart.assign(NCAMP + 1, 0);
    }
    vector<pair<size_t, int>> order(NQ);
    for (int q = 0; q < NQ; q++) {
        size_t n = 0;
        for (int t = 0; t < NT; t++) n += sBuf[t][q].size();
        order[q] = {n, q};
    }
    sort(order.begin(), order.end(), greater<>());
    // Each worker writes its own user_daily file as soon as no partitions are left for it, so
    // writing overlaps the other workers' last partitions. campaign_daily needs every worker's
    // counters and is written after the join.
    mkdirs(OUT);
    mkdirs(OUT + "/user_daily");
    mkdirs(OUT + "/campaign_daily");
    atomic<int> written{0};
    {
        atomic<size_t> next{0};
        auto work = [&](int tid) {
            for (;;) {
                size_t i = next.fetch_add(1, memory_order_relaxed);
                if (i >= (size_t)NQ) break;
                personPartition(order[i].second, tid);
            }
            if (!uRows[tid].empty()) {
                writeUserFile(OUT + "/user_daily/part-" + to_string(tid) + ".parquet", uRows[tid]);
                written++;
            }
        };
        vector<thread> th;
        for (int t = 1; t < NT; t++) th.emplace_back(work, t);
        work(0);
        for (auto& x : th) x.join();
    }
    logt("sessions+attribution+user_daily");
    writeCampaignFile(OUT + "/campaign_daily/part-0.parquet");
    if (written == 0) writeUserFile(OUT + "/user_daily/part-0.parquet", {});
    logt("write");
    if (VERBOSE) {
        string r = readSmall("/proc/self/smaps_rollup");
        size_t k = r.find("AnonHugePages");
        size_t k2 = r.find("Rss");
        for (size_t x : {k, k2})
            if (x != string::npos) fprintf(stderr, "%s", r.substr(x, r.find('\n', x) - x + 1).c_str());
    }
    fflush(stdout);
    fflush(stderr);
#ifdef SS_GCOV
    __gcov_dump();  // profile-guided-optimization training build: _exit skips the normal dump
#endif
    _exit(0);
}
#elif defined(SS_CIDTEST)
// Test driver: SIMD compact-ID encoder vs the scalar one on random strings.
int main() {
    initMasks();
    initCompact();
    u64 rng = 88172645463325252ULL;
    auto rnd = [&]() { rng ^= rng << 13; rng ^= rng >> 7; rng ^= rng << 17; return rng; };
    const char pool[] = "-0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ_abcdefghijklmnopqrstuvwxyz,./@[`{ \x7f\x80\xff\t";
    vector<char> buf(64);
    long bad = 0, n = 0;
    for (int it = 0; it < 3000000; it++) {
        size_t len = 1 + rnd() % 12, at = rnd() % 32;
        for (auto& c : buf) c = pool[rnd() % (sizeof(pool) - 1)];
        if (rnd() % 4) for (size_t i = 0; i < len; i++) buf[at + i] = pool[rnd() % 64];  // mostly valid
        u64 a = cid64(buf.data() + at, len), b = cid64f(buf.data() + at, len, buf.data() + 64);
        n++;
        if (a != b && ++bad < 5) printf("mismatch len %zu: %llx vs %llx\n", len, (unsigned long long)a, (unsigned long long)b);
    }
    printf("%ld cases, %ld mismatches\n", n, bad);
    return bad != 0;
}
#elif defined(SS_BENCH)
// Bench: scanner cost alone over every events file, mode 1 = old block scanner, 2 = line scanner,
// 3 = line scanner + timestamp parse + compact event id.
int main(int argc, char** argv) {
    NT = argc > 3 ? atoi(argv[3]) : 8;
    initMasks();
    initCompact();
    int mode = atoi(argv[2]);
    vector<string> paths = listCsv(string(argv[1]) + "/events");
    vector<MFile> files;
    for (auto& p : paths) files.push_back(mapFile(p));
    vector<Chunk> ch;
    makeChunks(files, 2 << 20, ch);
    for (auto& c : ch) for (const char* q = c.s; q < c.e; q += 4096) (void)*(volatile const char*)q;
    atomic<u64> acc{0};
    auto t0 = chrono::steady_clock::now();
    parallel_for(ch.size(), [&](size_t i, int) {
        u64 x = 0;
        auto cb = [&](const char** fp, const u32* fl) {
            x += fl[0] + fl[6];
            if (mode >= 3) {
                i64 t;
                if (mode != 5 && parseTs(fp[2], fl[2], t)) x += (u64)t;
                if (mode != 5 && parseTs(fp[3], fl[3], t)) x += (u64)t;
                if (mode != 6) x += cid64f(fp[0], fl[0], ch[i].lim);
            }
            if (mode >= 4) {
                x += cid64f(fp[1], fl[1], ch[i].lim);
                x += productNum(fp[5], fl[5]);
                u32 pr;
                if (fl[4] == 8 && parsePrice(fp[6], fl[6], pr)) x += pr;
            }
        };
        Chunk c = ch[i];
        snapChunk(c);
        if (mode == 1) scanLines<7>(c.s, c.e, cb); else scanLines2<7>(c.s, c.e, c.lim, cb);
        acc += x;
    });
    printf("mode %d: %.3f s (%llu)\n", mode, chrono::duration<double>(chrono::steady_clock::now() - t0).count(), (unsigned long long)acc.load());
    return 0;
}
#elif defined(SS_SCANTEST)
// Test driver: compares scanLines2<4> with the scalar splitter on every line of a file.
int main(int argc, char** argv) {
    string d = readSmall(argv[1]);
    vector<string> a, b;
    const char* s = d.data();
    const char* e = s + d.size();
    scanLines2<4>(s, e, e, [&](const char** fp, const u32* fl) {
        string r;
        for (int i = 0; i < 4; i++) r += "[" + string(fp[i], fl[i]) + "]";
        a.push_back(r);
    });
    const char* p = s;
    while (p < e) {
        const char* nl = (const char*)memchr(p, '\n', e - p);
        const char* le = nl ? nl : e;
        const char* fp[4];
        u32 fl[4];
        if (splitLine<4>(p, le, fp, fl)) {
            string r;
            for (int i = 0; i < 4; i++) r += "[" + string(fp[i], fl[i]) + "]";
            b.push_back(r);
        }
        p = nl ? nl + 1 : e;
    }
    size_t bad = a.size() != b.size();
    for (size_t i = 0; i < min(a.size(), b.size()); i++) bad += a[i] != b[i];
    printf("scanLines2 %zu lines, splitLine %zu lines, mismatches %zu\n", a.size(), b.size(), bad);
    return bad ? 1 : 0;
}
#else
// Test driver: one timestamp per line on stdin -> "BAD" or the parsed epoch ms.
int main() {
    char buf[1 << 16];
    while (fgets(buf, sizeof buf, stdin)) {
        size_t n = strlen(buf);
        if (n && buf[n - 1] == '\n') buf[--n] = 0;
        i64 t;
        if (parseTs(buf, (u32)n, t))
            printf("%lld\n", (long long)t);
        else
            printf("BAD\n");
    }
    return 0;
}
#endif
