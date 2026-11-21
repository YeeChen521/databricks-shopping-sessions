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
static const size_t BLK = 32u << 10;
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

struct Chunk {
    const char* s;    // first byte of the first line
    const char* e;    // one past the last byte (after a '\n', or the file end)
    const char* lim;  // end of the mapped file
};
static void makeChunks(const vector<MFile>& files, size_t target, vector<Chunk>& out) {
    for (auto& f : files) {
        if (f.n == 0) continue;
        const char* end = f.p + f.n;
        const char* h = (const char*)memchr(f.p, '\n', f.n);
        if (!h) continue;  // header only, no newline
        const char* s = h + 1;
        while (s < end) {
            const char* e = s + target;
            if (e >= end) {
                e = end;
            } else {
                const char* nl = (const char*)memchr(e, '\n', end - e);
                e = nl ? nl + 1 : end;
            }
            out.push_back(Chunk{s, e, end});
            s = e;
        }
    }
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

// F1/F2/F3 -> epoch ms. false = bad timestamp. F3 values beyond int64 map to INT64_MAX
// (outside every window).
static inline bool parseTs(const char* p, u32 n, i64& out) {
    if (n == 0) return false;
    if (n >= 20 && p[4] == '-') {
        if (n != 20 && n != 25) return false;
        int y1, y2, mo, d, h, mi, se;
        if (!dig2(p, y1) || !dig2(p + 2, y2) || !dig2(p + 5, mo) || p[7] != '-' || !dig2(p + 8, d) ||
            p[10] != 'T' || !dig2(p + 11, h) || p[13] != ':' || !dig2(p + 14, mi) || p[16] != ':' ||
            !dig2(p + 17, se))
            return false;
        int y = y1 * 100 + y2;
        if (mo < 1 || mo > 12 || d < 1 || h > 23 || mi > 59 || se > 59) return false;
        int md = MDAYS[mo];
        if (mo == 2 && (y % 4 == 0 && (y % 100 != 0 || y % 400 == 0))) md = 29;
        if (d > md) return false;
        i64 t = daysFromCivil(y, mo, d) * DAY + (i64)h * 3600000 + (i64)mi * 60000 + (i64)se * 1000;
        if (n == 20) {
            if (p[19] != 'Z') return false;
        } else {
            char sg = p[19];
            int oh, om;
            if ((sg != '+' && sg != '-') || !dig2(p + 20, oh) || p[22] != ':' || !dig2(p + 23, om)) return false;
            if (oh > 23 || om > 59) return false;
            i64 off = ((i64)oh * 60 + om) * 60000;
            t -= sg == '+' ? off : -off;
        }
        out = t;
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
static vector<string> g_catNames;
static vector<int> g_catRank;
static int g_unknownCat;
static KeyMap<u32> g_campaigns;
static vector<string> g_campNames, g_campChannels;
static int NCAMP;  // index NCAMP = organic

// id -> person name, only for IDs that appear in links; split into groups by the hash's top bits
static const int LGB = 6;
static const int NLG = 1 << LGB;
static vector<KeyMap<Key>> g_linkG;
static bool g_haveLinks = false;
static inline const Key* linkFind(Key k, u64 h) { return g_linkG[h >> (64 - LGB)].findH(k, h); }
static inline void linkPrefetch(u64 h) { g_linkG[h >> (64 - LGB)].prefetchH(h); }

static int PB = 9;  // partition bits
static int NP = 1 << 9;
static inline int partOf(u64 h) { return (int)(h >> (64 - PB)); }

struct ERec {  // event copy, partitioned by event_id
    Key eid;
    Key person;
    u32 ts;
    i32 ing;  // seconds relative to CWS
    u32 price;
    u16 cat;  // INVALID_CAT: kept copy would be dropped
    u16 pad;
};
struct SRec {  // surviving event, partitioned by person
    Key person;
    u32 ts;
    u32 price;
    u32 cat;
    u32 pad;
};
struct CRec {  // valid click, partitioned by person
    Key person;
    Key cid;
    u32 ts;
    u32 camp;
};
static_assert(sizeof(ERec) == 48, "ERec");
static_assert(sizeof(SRec) == 32, "SRec");
static_assert(sizeof(CRec) == 40, "CRec");

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
    for (auto& pr : prods) {
        bool ins;
        g_products.insert(pr.first, pr.second, ins);
    }
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
static void loadLinks() {
    vector<string> paths = listCsv(DATA + "/links");
    vector<MFile> files;
    for (auto& p : paths) files.push_back(mapFile(p));
    vector<Chunk> chunks;
    makeChunks(files, 256 << 10, chunks);
    vector<vector<Key>> pairs(NT);
    parallel_for(chunks.size(), [&](size_t ci, int tid) {
        auto& out = pairs[tid];
        const char* lim = chunks[ci].lim;
        scanLines<2>(chunks[ci].s, chunks[ci].e, [&](const char** fp, const u32* fl) {
            if (fl[0] && fl[1]) {
                out.push_back(packKey<true>(fp[0], fl[0], lim, tid));
                out.push_back(packKey<true>(fp[1], fl[1], lim, tid));
            }
        });
    });
    mergeLongs();
    logt("links parse");
    vector<size_t> posBase(NT + 1, 0);
    for (int t = 0; t < NT; t++) posBase[t + 1] = posBase[t] + pairs[t].size();
    size_t total = posBase[NT];
    if (total == 0) return;
    if (total >= (1ull << 32)) die("too many links");
    g_haveLinks = true;

    // 1. scatter key occurrences into groups
    struct Occ {
        Key k;
        u64 pos;
    };
    vector<vector<vector<Occ>>> gbuf(NT, vector<vector<Occ>>(NLG));
    parallel_for(NT, [&](size_t t, int) {
        auto& v = pairs[t];
        auto& gb = gbuf[t];
        size_t est = v.size() / NLG + 16;
        for (auto& x : gb) x.reserve(est + est / 4);
        for (size_t i = 0; i < v.size(); i++) gb[hk(v[i]) >> (64 - LGB)].push_back(Occ{v[i], posBase[t] + i});
    });
    // 2. intern each group: value.a = local id; nodeOf[pos] = group << 26 | local
    vector<u32> nodeOf(total);
    g_linkG.assign(NLG, KeyMap<Key>{});
    vector<vector<Key>> gkeys(NLG);
    parallel_for(NLG, [&](size_t g, int) {
        size_t cnt = 0;
        for (int t = 0; t < NT; t++) cnt += gbuf[t][g].size();
        KeyMap<Key>& m = g_linkG[g];
        m.initArena(cnt * 3 / 5 + 16);
        auto& keys = gkeys[g];
        for (int t = 0; t < NT; t++) {
            for (const Occ& o : gbuf[t][g]) {
                bool ins;
                Key* val = m.insert(o.k, Key{(u64)keys.size(), 0}, ins);
                if (ins) keys.push_back(o.k);
                nodeOf[o.pos] = ((u32)g << 26) | (u32)val->a;
            }
            vector<Occ>().swap(gbuf[t][g]);
        }
        if (keys.size() >= (1u << 26)) die("link group too large");
    });
    vector<u32> goff(NLG + 1, 0);
    for (int g = 0; g < NLG; g++) goff[g + 1] = goff[g] + (u32)gkeys[g].size();
    size_t nn = goff[NLG];
    logt("links intern");
    // 3. union-find; a root's parent is always the smaller index
    vector<u32> uf(nn);
    for (size_t i = 0; i < nn; i++) uf[i] = (u32)i;
    auto gid = [&](u32 x) { return goff[x >> 26] + (x & ((1u << 26) - 1)); };
    auto find = [&](u32 x) {
        while (uf[x] != x) {
            uf[x] = uf[uf[x]];
            x = uf[x];
        }
        return x;
    };
    for (size_t i = 0; i < total; i += 2) {
        u32 a = find(gid(nodeOf[i])), b = find(gid(nodeOf[i + 1]));
        if (a != b) {
            if (a < b) swap(a, b);
            uf[a] = b;
        }
    }
    for (size_t i = 0; i < nn; i++) uf[i] = uf[uf[i]];  // parent < child: one pass = roots
    // 4. smallest key per component
    vector<Key> best(nn, Key{0, 0});
    for (int g = 0; g < NLG; g++) {
        auto& keys = gkeys[g];
        for (size_t l = 0; l < keys.size(); l++) {
            u32 r = uf[goff[g] + l];
            if (isNull(best[r]) || keyLess(keys[l], best[r])) best[r] = keys[l];
        }
    }
    logt("links uf");
    // 5. names into the group tables
    parallel_for(NLG, [&](size_t g, int) {
        KeyMap<Key>& m = g_linkG[g];
        for (u64 i = 0; i <= m.mask; i++) {
            auto& sl = m.s[i];
            if (!isNull(sl.k)) sl.v = best[uf[goff[g] + (u32)sl.v.a]];
        }
    });
    for (auto& f : files)
        if (f.n) munmap((void*)f.p, f.n);
}

// ---------------------------------------------------------------------------------------------
// Events and clicks parsing
// ---------------------------------------------------------------------------------------------
static vector<vector<Stream<ERec>>> evBuf;  // [thread][partition]
static vector<vector<Stream<CRec>>> clBuf;  // [thread][partition]

static const int BATCH = 16;

struct EvItem {
    ERec r;
    Key uid, pid;
    u64 hu, hp;
};

static void parseEventChunk(const Chunk& c, int tid) {
    auto& out = evBuf[tid];
    Slab& slab = g_slabs[tid];
    const char* lim = c.lim;
    EvItem items[BATCH];
    int nb = 0;
    auto flush = [&]() {
        for (int i = 0; i < nb; i++) {
            EvItem& it = items[i];
            if (it.r.cat != INVALID_CAT) {
                if (g_haveLinks) {
                    const Key* p = linkFind(it.uid, it.hu);
                    it.r.person = p ? *p : it.uid;
                } else {
                    it.r.person = it.uid;
                }
                const u16* cat = g_products.findH(it.pid, it.hp);
                it.r.cat = cat ? *cat : (u16)g_unknownCat;
            }
            out[partOf(hk(it.r.eid))].push(it.r, slab);
        }
        nb = 0;
    };
    scanLines<7>(c.s, c.e, [&](const char** fp, const u32* fl) {
        if (fl[0] == 0) return;
        EvItem& it = items[nb];
        ERec& r = it.r;
        r.eid = packKey<false>(fp[0], fl[0], lim, tid);
        i64 ing;
        if (parseTs(fp[3], fl[3], ing)) {
            i64 s = fdiv(ing, 1000) - CWS / 1000;
            r.ing = s > INT32_MAX ? INT32_MAX : s < INT32_MIN ? INT32_MIN : (i32)s;
        } else {
            ing = INT64_MAX;
            r.ing = INT32_MAX;
        }
        r.cat = INVALID_CAT;
        r.person = Key{0, 0};
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
                it.uid = packKey<true>(fp[1], fl[1], lim, tid);
                it.pid = packKey<false>(fp[5], fl[5], lim, tid);
                it.hu = hk(it.uid);
                it.hp = hk(it.pid);
                if (g_haveLinks) linkPrefetch(it.hu);
                g_products.prefetchH(it.hp);
            }
        }
        if (++nb == BATCH) flush();
    });
    flush();
}

struct ClItem {
    CRec r;
    u64 hu;
};

static void parseClickChunk(const Chunk& c, int tid) {
    auto& out = clBuf[tid];
    Slab& slab = g_slabs[tid];
    const char* lim = c.lim;
    ClItem items[BATCH];
    int nb = 0;
    auto flush = [&]() {
        for (int i = 0; i < nb; i++) {
            ClItem& it = items[i];
            if (g_haveLinks) {
                const Key* p = linkFind(it.r.person, it.hu);
                if (p) {
                    it.r.person = *p;
                    it.hu = hk(*p);
                }
            }
            out[partOf(it.hu)].push(it.r, slab);
        }
        nb = 0;
    };
    scanLines<4>(c.s, c.e, [&](const char** fp, const u32* fl) {
        if (fl[0] == 0 || fl[1] == 0 || fl[2] == 0) return;
        i64 ts;
        if (!parseTs(fp[3], fl[3], ts) || ts < CWS || ts >= WE) return;
        const u32* camp = g_campaigns.find(packKey<false>(fp[2], fl[2], lim, tid));
        if (!camp) return;
        ClItem& it = items[nb];
        it.r.person = packKey<true>(fp[1], fl[1], lim, tid);
        it.r.cid = packKey<true>(fp[0], fl[0], lim, tid);
        it.r.ts = (u32)(ts - CWS);
        it.r.camp = *camp;
        it.hu = hk(it.r.person);
        if (g_haveLinks) linkPrefetch(it.hu);
        if (++nb == BATCH) flush();
    });
    flush();
}

// ---------------------------------------------------------------------------------------------
// Dedup
// ---------------------------------------------------------------------------------------------
static vector<vector<Stream<SRec>>> sBuf;  // [thread][person partition]
struct DedupSlot {
    Key k;
    const ERec* r;
};
static vector<vector<DedupSlot>> g_dedupTab;  // [thread]

static void dedupPartition(int p, int tid) {
    size_t n = 0;
    for (int t = 0; t < NT; t++) n += evBuf[t][p].size();
    if (n == 0) return;
    size_t sz = 64;
    while (sz < n * 2) sz <<= 1;
    auto& tabv = g_dedupTab[tid];
    if (tabv.size() < sz) tabv.resize(sz);
    DedupSlot* tab = tabv.data();
    memset((void*)tab, 0, sz * sizeof(DedupSlot));
    u64 mask = sz - 1;
    const int PF = 8;
    u64 hs[Stream<ERec>::CAP];
    // pass 1: winner per event_id (smallest ingest)
    for (int t = 0; t < NT; t++) {
        evBuf[t][p].eachBlock([&](const ERec* rs, size_t cnt) {
            for (size_t j = 0; j < cnt; j++) hs[j] = hk(rs[j].eid);
            for (size_t j = 0; j < cnt; j++) {
                if (j + PF < cnt) __builtin_prefetch(&tab[hs[j + PF] & mask]);
                const ERec& r = rs[j];
                u64 i = hs[j] & mask;
                for (;;) {
                    DedupSlot& x = tab[i];
                    if (x.r == nullptr) {
                        x.k = r.eid;
                        x.r = &r;
                        break;
                    }
                    if (x.k == r.eid) {
                        if (r.ing < x.r->ing) x.r = &r;
                        break;
                    }
                    i = (i + 1) & mask;
                }
            }
        });
    }
    // pass 2: records in storage order; emit the valid winners
    auto& out = sBuf[tid];
    Slab& slab = g_slabs[tid];
    for (int t = 0; t < NT; t++) {
        evBuf[t][p].eachBlock([&](const ERec* rs, size_t cnt) {
            for (size_t j = 0; j < cnt; j++) hs[j] = hk(rs[j].eid);
            for (size_t j = 0; j < cnt; j++) {
                if (j + PF < cnt) __builtin_prefetch(&tab[hs[j + PF] & mask]);
                const ERec& r = rs[j];
                if (r.cat == INVALID_CAT) continue;
                u64 i = hs[j] & mask;
                while (!(tab[i].k == r.eid)) i = (i + 1) & mask;
                if (tab[i].r == &r) out[partOf(hk(r.person))].push(SRec{r.person, r.ts, r.price, r.cat, 0}, slab);
            }
        });
    }
    for (int t = 0; t < NT; t++) evBuf[t][p].release(slab);
}

// ---------------------------------------------------------------------------------------------
// Sessions and attribution
// ---------------------------------------------------------------------------------------------
struct URow {
    Key person;
    i32 day;
    u32 sessions;
    u32 events;
    i32 topcat;  // -1 = null
    i64 revenue;
};
struct CampAgg {
    i64 sessions, purch, revenue, assisted;
};
static vector<vector<URow>> uRows;
static vector<vector<CampAgg>> campAgg;

struct PEv {
    u32 ts;
    u32 price;
    u32 cat;
};
struct PCl {
    u32 ts;
    u32 camp;
    Key cid;
};

struct Scratch {
    vector<i64> catRev;
    vector<int> catTouched;
    vector<u32> campCnt;
    vector<u32> active;
    vector<u32> activePos;
    KeyMap<u32> pm;
    vector<Key> pkeys;
    vector<u32> cnt, off, pos, pidx, coff, cidx;
    vector<PEv> ev;
    vector<PCl> cl;
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
    KeyMap<u32>& pm = sc.pm;
    pm.initOwn(ns + 16);
    auto& pkeys = sc.pkeys;
    auto& cnt = sc.cnt;
    pkeys.clear();
    cnt.clear();
    auto& pidx = sc.pidx;
    if (pidx.size() < ns) pidx.resize(ns);
    {
        size_t i = 0;
        for (int t = 0; t < NT; t++)
            sBuf[t][q].each([&](const SRec& r) {
                bool ins;
                u32* v = pm.insert(r.person, (u32)pkeys.size(), ins);
                if (ins) {
                    pkeys.push_back(r.person);
                    cnt.push_back(0);
                }
                cnt[*v]++;
                pidx[i++] = *v;
            });
    }
    size_t np = pkeys.size();
    auto& off = sc.off;
    off.assign(np + 1, 0);
    for (size_t k = 0; k < np; k++) off[k + 1] = off[k] + cnt[k];
    auto& ev = sc.ev;
    if (ev.size() < ns) ev.resize(ns);
    {
        auto& pos = sc.pos;
        pos.assign(off.begin(), off.end() - 1);
        size_t i = 0;
        for (int t = 0; t < NT; t++)
            sBuf[t][q].each([&](const SRec& r) { ev[pos[pidx[i++]]++] = PEv{r.ts, r.price, r.cat}; });
    }
    auto& coff = sc.coff;
    coff.assign(np + 1, 0);
    auto& cl = sc.cl;
    if (nc) {
        auto& cidx = sc.cidx;
        if (cidx.size() < nc) cidx.resize(nc);
        size_t i = 0;
        for (int t = 0; t < NT; t++)
            clBuf[t][q].each([&](const CRec& r) {
                const u32* v = pm.find(r.person);
                u32 k = v ? *v : UINT32_MAX;
                cidx[i++] = k;
                if (v) coff[k + 1]++;
            });
        for (size_t k = 0; k < np; k++) coff[k + 1] += coff[k];
        if (cl.size() < coff[np]) cl.resize(coff[np]);
        auto& pos = sc.pos;
        pos.assign(coff.begin(), coff.end() - 1);
        i = 0;
        for (int t = 0; t < NT; t++)
            clBuf[t][q].each([&](const CRec& r) {
                u32 k = cidx[i++];
                if (k != UINT32_MAX) cl[pos[k]++] = PCl{r.ts, r.camp, r.cid};
            });
    }
    auto& rows = uRows[tid];
    auto& agg = campAgg[tid];
    auto& catRev = sc.catRev;
    auto& touched = sc.catTouched;
    auto& campCnt = sc.campCnt;
    auto& active = sc.active;
    auto& activePos = sc.activePos;
    const i64 cwsDay = CWS;

    for (size_t k = 0; k < np; k++) {
        PEv* e = ev.data() + off[k];
        size_t n = off[k + 1] - off[k];
        if (n <= 32) {
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
        size_t m = coff[k + 1] - coff[k];
        PCl* c = m ? cl.data() + coff[k] : nullptr;
        if (m > 1) {
            sort(c, c + m, [](const PCl& a, const PCl& b) {
                if (a.ts != b.ts) return a.ts < b.ts;
                return keyLess(b.cid, a.cid);  // click_id descending: last = smallest
            });
        }
        size_t lo = 0, hi = 0;

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
            int day = (int)fdiv(cwsDay + start, DAY);
            if (day != curDay) {
                flushDay();
                curDay = day;
                row = URow{pkeys[k], day, 0, 0, -1, 0};
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

            while (hi < m && c[hi].ts <= start) {
                u32 cp = c[hi].camp;
                if (campCnt[cp]++ == 0) {
                    activePos[cp] = (u32)active.size();
                    active.push_back(cp);
                }
                hi++;
            }
            u32 from = start - (u32)WEEK;  // start >= WEEK for events in the window
            while (lo < hi && c[lo].ts < from) {
                u32 cp = c[lo].camp;
                if (--campCnt[cp] == 0) {
                    u32 ps = activePos[cp];
                    u32 last = active.back();
                    active[ps] = last;
                    activePos[last] = ps;
                    active.pop_back();
                }
                lo++;
            }
            int di = day - (int)DAY0;
            u32 credited = hi > lo ? c[hi - 1].camp : (u32)NCAMP;
            CampAgg& a = agg[(size_t)credited * NDAYS + di];
            a.sessions++;
            a.revenue += rev;
            if (rev > 0) a.purch++;
            for (u32 cp : active) agg[(size_t)cp * NDAYS + di].assisted++;
        }
        flushDay();
        for (; lo < hi; lo++) campCnt[c[lo].camp] = 0;
        active.clear();
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
        if (!isLong(r.person)) {
            unsigned char buf[16];
            u64 a = __builtin_bswap64(r.person.a), b = __builtin_bswap64(r.person.b);
            memcpy(buf, &a, 8);
            memcpy(buf + 8, &b, 8);
            pu.str((const char*)buf, buf[15]);
        } else {
            pu.str(keyStr(r.person));
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
            CampAgg a{0, 0, 0, 0};
            for (int t = 0; t < NT; t++) {
                const CampAgg& x = campAgg[t][(size_t)cp * NDAYS + di];
                a.sessions += x.sessions;
                a.purch += x.purch;
                a.revenue += x.revenue;
                a.assisted += x.assisted;
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
    g_longs.assign(NT, {});
    g_slabs.assign(NT, Slab{});
    initMasks();

    // inputs
    vector<string> evPaths = listCsv(DATA + "/events"), clPaths = listCsv(DATA + "/clicks");
    vector<MFile> evFiles, clFiles;
    for (auto& p : evPaths) evFiles.push_back(mapFile(p));
    for (auto& p : clPaths) clFiles.push_back(mapFile(p));
    size_t evBytes = 0, clBytes = 0;
    for (auto& f : evFiles) evBytes += f.n;
    for (auto& f : clFiles) clBytes += f.n;
    // partitions: ~40k events each
    PB = 8;
    while (PB < 12 && (evBytes / 80) >> PB > 40000) PB++;
    NP = 1 << PB;
    g_arena.init(evBytes * 2 + clBytes * 2 + ((size_t)NT * NP * 3 * BLK) + (1ull << 30));

    loadManifest();
    loadLookups();
    logt("lookups");
    loadLinks();
    logt("links");

    vector<Chunk> evChunks, clChunks;
    makeChunks(evFiles, 2 << 20, evChunks);
    makeChunks(clFiles, 2 << 20, clChunks);
    evBuf.assign(NT, vector<Stream<ERec>>(NP));
    clBuf.assign(NT, vector<Stream<CRec>>(NP));
    sBuf.assign(NT, vector<Stream<SRec>>(NP));
    logt("chunks");

    size_t nE = evChunks.size();
    parallel_for(nE + clChunks.size(), [&](size_t i, int tid) {
        if (i < nE)
            parseEventChunk(evChunks[i], tid);
        else
            parseClickChunk(clChunks[i - nE], tid);
    });
    mergeLongs();
    logt("parse events+clicks");

    g_dedupTab.assign(NT, {});
    parallel_for(NP, [&](size_t p, int tid) { dedupPartition((int)p, tid); });
    logt("dedup");

    uRows.assign(NT, {});
    campAgg.assign(NT, vector<CampAgg>((size_t)(NCAMP + 1) * NDAYS, CampAgg{0, 0, 0, 0}));
    scratch.resize(NT);
    for (auto& s : scratch) {
        s.catRev.assign(g_catNames.size(), 0);
        s.campCnt.assign(NCAMP + 1, 0);
        s.activePos.assign(NCAMP + 1, 0);
    }
    vector<pair<size_t, int>> order(NP);
    for (int q = 0; q < NP; q++) {
        size_t n = 0;
        for (int t = 0; t < NT; t++) n += sBuf[t][q].size();
        order[q] = {n, q};
    }
    sort(order.begin(), order.end(), greater<>());
    parallel_for(NP, [&](size_t i, int tid) { personPartition(order[i].second, tid); });
    logt("sessions+attribution");

    mkdirs(OUT);
    mkdirs(OUT + "/user_daily");
    mkdirs(OUT + "/campaign_daily");
    atomic<int> written{0};
    parallel_for(NT + 1, [&](size_t t, int) {
        if (t == (size_t)NT) {
            writeCampaignFile(OUT + "/campaign_daily/part-0.parquet");
            return;
        }
        if (uRows[t].empty()) return;
        writeUserFile(OUT + "/user_daily/part-" + to_string(t) + ".parquet", uRows[t]);
        written++;
    });
    if (written == 0) writeUserFile(OUT + "/user_daily/part-0.parquet", {});
    logt("write");
    fflush(stdout);
    fflush(stderr);
    _exit(0);
}
