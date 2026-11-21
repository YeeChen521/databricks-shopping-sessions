// Shopping Sessions: native pipeline.
//
// Reads $DATA_DIR (default /data), writes $OUTPUT_DIR/{user_daily,campaign_daily}/*.parquet.
// Optional positional arguments: DATA_DIR OUTPUT_DIR.
//
// Stages
//   0. manifest, products, campaigns
//   1. links: parallel parse -> union-find -> id -> person-name table
//   2. events + clicks: parallel parse over newline-aligned chunks of every file.
//        events are scattered by hash(event_id), clicks by hash(person)
//   3. per event partition: dedup on event_id (smallest ingest_ts), then scatter the
//        surviving (valid, not late) kept copies by hash(person)
//   4. per person partition: sort, sessionize, attribute, aggregate
//   5. Parquet output (one user_daily file per thread)
//
// IDs are packed into 16-byte keys: up to 15 bytes are stored exactly (big-endian, last byte
// is the length), so comparing keys as integers is byte-wise string order. Longer IDs get a
// 120-bit hash plus a side table holding the string (used for ordering and output).

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
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>

using namespace std;
typedef int64_t i64;
typedef uint64_t u64;
typedef uint32_t u32;
typedef uint16_t u16;
typedef uint8_t u8;

static const i64 DAY = 86400000LL;
static const i64 GAP = 1800000LL;
static const i64 WEEK = 7 * DAY;

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
    exit(1);
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
// Keys
// ---------------------------------------------------------------------------------------------
struct Key {
    u64 a, b;
};
static inline bool operator==(Key x, Key y) { return x.a == y.a && x.b == y.b; }
static inline bool isLong(Key k) { return (k.b & 0xFF) == 0xFF; }
static inline bool isNull(Key k) { return (k.a | k.b) == 0; }

static inline u64 mix64(u64 x) {
    x ^= x >> 30;
    x *= 0xbf58476d1ce4e5b9ULL;
    x ^= x >> 27;
    x *= 0x94d049bb133111ebULL;
    x ^= x >> 31;
    return x;
}
static inline u64 hk(Key k) { return mix64(k.a * 0x9E3779B97F4A7C15ULL ^ k.b); }

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

// Long-string registry: per thread while parsing, merged before use.
struct KH {
    size_t operator()(const Key& k) const { return hk(k); }
};
static vector<vector<pair<Key, string>>> g_longs;  // [thread]
static unordered_map<Key, string, KH> g_longmap;
static void mergeLongs() {
    for (auto& v : g_longs) {
        for (auto& kv : v) g_longmap.emplace(kv.first, std::move(kv.second));
        v.clear();
    }
}

template <bool REG>
static inline Key packKey(const char* p, size_t n, int tid) {
    if (n <= 15) {
        unsigned char buf[16];
        memset(buf, 0, 16);
        memcpy(buf, p, n);
        buf[15] = (unsigned char)n;
        u64 a, b;
        memcpy(&a, buf, 8);
        memcpy(&b, buf + 8, 8);
        return Key{__builtin_bswap64(a), __builtin_bswap64(b)};
    }
    Key k{hashBytes(p, n, 1), hashBytes(p, n, 2) | 0xFF};
    if (REG) g_longs[tid].emplace_back(k, string(p, n));
    return k;
}

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
template <class V>
struct KeyMap {
    struct Slot {
        Key k;
        V v;
    };
    vector<Slot> s;
    u64 mask = 0;
    size_t n = 0;
    void init(size_t cap) {
        size_t sz = 16;
        while (sz < cap * 2) sz <<= 1;
        s.assign(sz, Slot{Key{0, 0}, V{}});
        mask = sz - 1;
        n = 0;
    }
    inline const V* find(Key k) const {
        u64 i = hk(k) & mask;
        for (;;) {
            const Slot& x = s[i];
            if (x.k == k) return &x.v;
            if (isNull(x.k)) return nullptr;
            i = (i + 1) & mask;
        }
    }
    inline void prefetch(Key k) const { __builtin_prefetch(&s[hk(k) & mask]); }
    // returns pointer to value; inserted=true if new
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
                n++;
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
    string path;
};
static MFile mapFile(const string& path) {
    MFile f;
    f.path = path;
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
    const char* s;
    const char* e;
};
// Splits the data lines (after each file's header line) into newline-aligned chunks.
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
            out.push_back(Chunk{s, e});
            s = e;
        }
    }
}

// ---------------------------------------------------------------------------------------------
// CSV line splitting per rules.md
// ---------------------------------------------------------------------------------------------
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
        p++;  // comma
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

// Parses F1/F2/F3. Returns false for a bad timestamp; F3 values that do not fit in int64 are
// reported as "ok" with a sentinel far outside any window.
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
    // F3: digits only
    u32 i = 0;
    while (i < n && p[i] == '0') i++;
    u32 nd = n - i;
    for (u32 j = i; j < n; j++)
        if ((unsigned)((unsigned char)p[j] - '0') > 9) return false;
    // leading zeros were digits; verify all-zero prefix is digits (it is: '0')
    if (nd > 19) {
        out = INT64_MAX;  // outside every window
        return true;
    }
    u64 v = 0;
    for (u32 j = i; j < n; j++) v = v * 10 + (unsigned)(p[j] - '0');
    out = v > (u64)INT64_MAX ? INT64_MAX : (i64)v;
    return true;
}

// ---------------------------------------------------------------------------------------------
// Globals
// ---------------------------------------------------------------------------------------------
static string DATA, OUT;
static i64 WS, WE, CWS;  // window start/end, click window start
static i64 DAY0;
static int NDAYS;

static KeyMap<u16> g_products;     // product key -> category index
static vector<string> g_catNames;  // index -> name
static vector<int> g_catRank;      // index -> rank in byte-wise order
static int g_unknownCat;
static KeyMap<u32> g_campaigns;  // campaign key -> index
static vector<string> g_campNames, g_campChannels;
static int NCAMP;  // index NCAMP = organic

static KeyMap<Key> g_link;  // id -> person name (only for IDs in links)
static bool g_haveLinks = false;

static inline Key resolvePerson(Key uid) {
    if (g_haveLinks) {
        const Key* p = g_link.find(uid);
        if (p) return *p;
    }
    return uid;
}

static const int PBITS = 9;
static const int NP = 1 << PBITS;
static inline int partOf(u64 h) { return (int)(h >> (64 - PBITS)); }

// Records
struct ERec {  // event copy, partitioned by event_id
    Key eid;
    i64 ing;
    Key person;
    i64 ts;
    u32 price;
    u16 cat;
    u8 valid;
};
struct SRec {  // surviving event, partitioned by person
    Key person;
    i64 ts;
    u32 price;
    u16 cat;
};
struct CRec {  // valid click, partitioned by person
    Key person;
    Key cid;
    i64 ts;
    u32 camp;
};

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

static void loadManifest() {
    string m = readSmall(DATA + "/manifest.json");
    auto get = [&](const char* name) -> i64 {
        size_t k = m.find(string("\"") + name + "\"");
        if (k == string::npos) die(string("manifest missing ") + name);
        size_t c = m.find(':', k);
        size_t q1 = m.find('"', c + 1);
        size_t q2 = m.find('"', q1 + 1);
        string v = m.substr(q1 + 1, q2 - q1 - 1);
        i64 t;
        if (!parseTs(v.data(), (u32)v.size(), t)) die("bad manifest timestamp " + v);
        return t;
    };
    WS = get("window_start");
    WE = get("window_end");
    CWS = WS - WEEK;
    auto fdiv = [](i64 a, i64 b) { return a >= 0 ? a / b : -((-a + b - 1) / b); };
    DAY0 = fdiv(WS, DAY);
    NDAYS = (int)(fdiv(WE - 1, DAY) - DAY0 + 1);
    if (NDAYS < 1) NDAYS = 1;
}

static void loadLookups() {
    // products
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
        prods.emplace_back(packKey<false>(fp[0], fl[0], 0), (u16)ci);
    });
    {
        auto it = catIdx.find("unknown");
        if (it == catIdx.end()) {
            g_unknownCat = (int)g_catNames.size();
            g_catNames.push_back("unknown");
        } else
            g_unknownCat = it->second;
    }
    if (g_catNames.size() > 65000) die("too many categories");
    g_products.init(prods.size() + 1);
    for (auto& pr : prods) {
        bool ins;
        g_products.insert(pr.first, pr.second, ins);
    }
    vector<int> order(g_catNames.size());
    for (size_t i = 0; i < order.size(); i++) order[i] = (int)i;
    sort(order.begin(), order.end(), [](int a, int b) { return g_catNames[a] < g_catNames[b]; });
    g_catRank.assign(order.size(), 0);
    for (size_t r = 0; r < order.size(); r++) g_catRank[order[r]] = (int)r;

    // campaigns
    string ct = readSmall(DATA + "/campaigns.csv");
    vector<pair<Key, u32>> camps;
    forEachLine(ct, [&](const char* s, const char* e) {
        const char* fp[2];
        u32 fl[2];
        if (!splitLine<2>(s, e, fp, fl) || fl[0] == 0) return;
        camps.emplace_back(packKey<false>(fp[0], fl[0], 0), (u32)g_campNames.size());
        g_campNames.emplace_back(fp[0], fl[0]);
        g_campChannels.emplace_back(fp[1], fl[1]);
    });
    NCAMP = (int)g_campNames.size();
    g_campaigns.init(camps.size() + 1);
    for (auto& c : camps) {
        bool ins;
        g_campaigns.insert(c.first, c.second, ins);
    }
}

// ---------------------------------------------------------------------------------------------
// Links
// ---------------------------------------------------------------------------------------------
static vector<u32> uf;
static u32 ufFind(u32 x) {
    while (uf[x] != x) {
        uf[x] = uf[uf[x]];
        x = uf[x];
    }
    return x;
}

static void loadLinks() {
    vector<string> paths = listCsv(DATA + "/links");
    vector<MFile> files;
    for (auto& p : paths) files.push_back(mapFile(p));
    vector<Chunk> chunks;
    makeChunks(files, 1 << 20, chunks);
    vector<vector<Key>> pairs(NT);
    parallel_for(chunks.size(), [&](size_t ci, int tid) {
        auto& out = pairs[tid];
        const char* s = chunks[ci].s;
        const char* end = chunks[ci].e;
        const char* fp[2];
        u32 fl[2];
        while (s < end) {
            const char* nl = (const char*)memchr(s, '\n', end - s);
            const char* e = nl ? nl : end;
            if (splitLine<2>(s, e, fp, fl) && fl[0] && fl[1]) {
                out.push_back(packKey<true>(fp[0], fl[0], tid));
                out.push_back(packKey<true>(fp[1], fl[1], tid));
            }
            s = nl ? nl + 1 : end;
        }
    });
    mergeLongs();
    size_t total = 0;
    for (auto& v : pairs) total += v.size();
    if (total == 0) return;
    g_haveLinks = true;

    KeyMap<u32> node;
    node.init(total);
    vector<Key> keys;
    keys.reserve(total);
    uf.clear();
    uf.reserve(total);
    auto id = [&](Key k) -> u32 {
        bool ins;
        u32* v = node.insert(k, (u32)keys.size(), ins);
        if (ins) {
            keys.push_back(k);
            uf.push_back(*v);
        }
        return *v;
    };
    for (auto& v : pairs) {
        for (size_t i = 0; i < v.size(); i += 2) {
            u32 a = ufFind(id(v[i])), b = ufFind(id(v[i + 1]));
            if (a != b) {
                if (a < b) swap(a, b);
                uf[a] = b;
            }
        }
    }
    size_t nn = keys.size();
    vector<Key> best(nn, Key{0, 0});
    for (size_t i = 0; i < nn; i++) {
        u32 r = ufFind((u32)i);
        if (isNull(best[r]) || keyLess(keys[i], best[r])) best[r] = keys[i];
    }
    g_link.init(nn);
    for (size_t i = 0; i < nn; i++) {
        bool ins;
        g_link.insert(keys[i], best[ufFind((u32)i)], ins);
    }
    for (auto& f : files)
        if (f.n) munmap((void*)f.p, f.n);
}

// ---------------------------------------------------------------------------------------------
// Events and clicks parsing
// ---------------------------------------------------------------------------------------------
static vector<vector<vector<ERec>>> evBuf;  // [thread][partition]
static vector<vector<vector<CRec>>> clBuf;  // [thread][partition]

static inline bool parsePrice(const char* p, u32 n, u32& out) {
    u32 i = 0;
    while (i < n && p[i] == '0') i++;
    for (u32 j = i; j < n; j++)
        if ((unsigned)((unsigned char)p[j] - '0') > 9) return false;
    if (n == 0) return false;
    u32 nd = n - i;
    if (nd == 0 || nd > 8) return false;
    u32 v = 0;
    for (u32 j = i; j < n; j++) v = v * 10 + (unsigned)(p[j] - '0');
    if (v < 1 || v > 10000000) return false;
    out = v;
    return true;
}

static void parseEventChunk(const Chunk& c, int tid) {
    auto& out = evBuf[tid];
    const char* s = c.s;
    const char* end = c.e;
    const char* fp[7];
    u32 fl[7];
    while (s < end) {
        const char* nl = (const char*)memchr(s, '\n', end - s);
        const char* e = nl ? nl : end;
        if (splitLine<7>(s, e, fp, fl) && fl[0] != 0) {
            ERec r;
            r.eid = packKey<false>(fp[0], fl[0], tid);
            if (!parseTs(fp[3], fl[3], r.ing)) r.ing = INT64_MAX;
            r.valid = 0;
            r.person = Key{0, 0};
            r.ts = 0;
            r.price = 0;
            r.cat = 0;
            i64 ts;
            if (fl[1] != 0 && fl[5] != 0 && parseTs(fp[2], fl[2], ts) && ts >= WS && ts < WE) {
                int typ = -1;
                if (fl[4] == 4) {
                    if (memcmp(fp[4], "view", 4) == 0 || memcmp(fp[4], "cart", 4) == 0) typ = 0;
                } else if (fl[4] == 8 && memcmp(fp[4], "purchase", 8) == 0)
                    typ = 1;
                u32 price = 0;
                bool ok = typ >= 0 && (typ == 0 || parsePrice(fp[6], fl[6], price));
                if (ok && !(r.ing - ts > DAY)) {
                    r.valid = 1;
                    r.ts = ts;
                    r.price = price;
                    r.person = resolvePerson(packKey<true>(fp[1], fl[1], tid));
                    const u16* cat = g_products.find(packKey<false>(fp[5], fl[5], tid));
                    r.cat = cat ? *cat : (u16)g_unknownCat;
                }
            }
            out[partOf(hk(r.eid))].push_back(r);
        }
        s = nl ? nl + 1 : end;
    }
}

static void parseClickChunk(const Chunk& c, int tid) {
    auto& out = clBuf[tid];
    const char* s = c.s;
    const char* end = c.e;
    const char* fp[4];
    u32 fl[4];
    while (s < end) {
        const char* nl = (const char*)memchr(s, '\n', end - s);
        const char* e = nl ? nl : end;
        if (splitLine<4>(s, e, fp, fl) && fl[0] != 0 && fl[1] != 0 && fl[2] != 0) {
            i64 ts;
            if (parseTs(fp[3], fl[3], ts) && ts >= CWS && ts < WE) {
                const u32* camp = g_campaigns.find(packKey<false>(fp[2], fl[2], tid));
                if (camp) {
                    CRec r;
                    r.person = resolvePerson(packKey<true>(fp[1], fl[1], tid));
                    r.cid = packKey<true>(fp[0], fl[0], tid);
                    r.ts = ts;
                    r.camp = *camp;
                    out[partOf(hk(r.person))].push_back(r);
                }
            }
        }
        s = nl ? nl + 1 : end;
    }
}

// ---------------------------------------------------------------------------------------------
// Dedup
// ---------------------------------------------------------------------------------------------
static vector<vector<vector<SRec>>> sBuf;  // [thread][person partition]

static void dedupPartition(int p, int tid) {
    size_t n = 0;
    for (int t = 0; t < NT; t++) n += evBuf[t][p].size();
    if (n == 0) return;
    struct Slot {
        Key k;
        const ERec* r;
    };
    size_t sz = 16;
    while (sz < n * 2) sz <<= 1;
    vector<Slot> tab(sz, Slot{Key{0, 0}, nullptr});
    u64 mask = sz - 1;
    for (int t = 0; t < NT; t++) {
        for (const ERec& r : evBuf[t][p]) {
            u64 i = hk(r.eid) & mask;
            for (;;) {
                Slot& x = tab[i];
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
    }
    auto& out = sBuf[tid];
    for (auto& x : tab) {
        if (x.r && x.r->valid) {
            const ERec& r = *x.r;
            out[partOf(hk(r.person))].push_back(SRec{r.person, r.ts, r.price, r.cat});
        }
    }
}

// ---------------------------------------------------------------------------------------------
// Sessions and attribution
// ---------------------------------------------------------------------------------------------
struct URow {
    Key person;
    int day;
    u32 sessions;
    u32 events;
    i64 revenue;
    int topcat;  // -1 = null
};
struct CampAgg {
    i64 sessions, purch, revenue, assisted;
};
static vector<vector<URow>> uRows;       // [thread]
static vector<vector<CampAgg>> campAgg;  // [thread][(NCAMP+1)*NDAYS]

struct PEv {
    i64 ts;
    u32 price;
    u16 cat;
};
struct PCl {
    i64 ts;
    Key cid;
    u32 camp;
};

struct Scratch {
    vector<i64> catRev;
    vector<int> catTouched;
    vector<u32> campCnt;
    vector<u32> active;     // active campaigns list
    vector<u32> activePos;  // position in active list
};
static vector<Scratch> scratch;

static inline void floorDay(i64 ts, int& day) { day = (int)(ts >= 0 ? ts / DAY : -((-ts + DAY - 1) / DAY)); }

static void personPartition(int q, int tid) {
    size_t ns = 0, nc = 0;
    for (int t = 0; t < NT; t++) {
        ns += sBuf[t][q].size();
        nc += clBuf[t][q].size();
    }
    if (ns == 0) return;
    Scratch& sc = scratch[tid];
    KeyMap<u32> pm;
    pm.init(ns + 16);
    vector<Key> pkeys;
    vector<u32> cnt;
    vector<u32> pidx(ns);
    {
        size_t i = 0;
        for (int t = 0; t < NT; t++)
            for (const SRec& r : sBuf[t][q]) {
                bool ins;
                u32* v = pm.insert(r.person, (u32)pkeys.size(), ins);
                if (ins) {
                    pkeys.push_back(r.person);
                    cnt.push_back(0);
                }
                cnt[*v]++;
                pidx[i++] = *v;
            }
    }
    size_t np = pkeys.size();
    vector<u32> off(np + 1, 0);
    for (size_t k = 0; k < np; k++) off[k + 1] = off[k] + cnt[k];
    vector<PEv> ev(ns);
    {
        vector<u32> pos(off.begin(), off.end() - 1);
        size_t i = 0;
        for (int t = 0; t < NT; t++)
            for (const SRec& r : sBuf[t][q]) ev[pos[pidx[i++]]++] = PEv{r.ts, r.price, r.cat};
    }
    // clicks
    vector<u32> coff(np + 1, 0);
    vector<PCl> cl;
    if (nc) {
        vector<u32> cidx(nc);
        size_t i = 0;
        for (int t = 0; t < NT; t++)
            for (const CRec& r : clBuf[t][q]) {
                const u32* v = pm.find(r.person);
                u32 k = v ? *v : UINT32_MAX;
                cidx[i++] = k;
                if (v) coff[k + 1]++;
            }
        for (size_t k = 0; k < np; k++) coff[k + 1] += coff[k];
        cl.resize(coff[np]);
        vector<u32> pos(coff.begin(), coff.end() - 1);
        i = 0;
        for (int t = 0; t < NT; t++)
            for (const CRec& r : clBuf[t][q]) {
                u32 k = cidx[i++];
                if (k != UINT32_MAX) cl[pos[k]++] = PCl{r.ts, r.cid, r.camp};
            }
    }
    auto& rows = uRows[tid];
    auto& agg = campAgg[tid];
    auto& catRev = sc.catRev;
    auto& touched = sc.catTouched;
    auto& campCnt = sc.campCnt;
    auto& active = sc.active;
    auto& activePos = sc.activePos;

    for (size_t k = 0; k < np; k++) {
        PEv* e = ev.data() + off[k];
        size_t n = off[k + 1] - off[k];
        if (n <= 24) {
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
        PCl* c = cl.empty() ? nullptr : cl.data() + coff[k];
        size_t m = cl.empty() ? 0 : coff[k + 1] - coff[k];
        if (m > 1) {
            sort(c, c + m, [](const PCl& a, const PCl& b) {
                if (a.ts != b.ts) return a.ts < b.ts;
                return keyLess(b.cid, a.cid);  // click_id descending: last = smallest
            });
        }
        size_t lo = 0, hi = 0;  // click window [lo, hi)

        // user-day accumulation
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
            i64 start = e[i].ts;
            u32 events = 0;
            i64 rev = 0;
            i64 prev = start;
            int day;
            floorDay(start, day);
            if (day != curDay) {
                flushDay();
                curDay = day;
                row = URow{pkeys[k], day, 0, 0, 0, -1};
            }
            while (i < n && e[i].ts - prev <= GAP) {
                prev = e[i].ts;
                events++;
                if (e[i].price) {
                    rev += e[i].price;
                    int ci = e[i].cat;
                    if (catRev[ci] == 0) touched.push_back(ci);
                    catRev[ci] += e[i].price;
                }
                i++;
            }
            row.sessions++;
            row.events += events;
            row.revenue += rev;

            // attribution: clicks with start - WEEK <= ts <= start
            while (hi < m && c[hi].ts <= start) {
                u32 cp = c[hi].camp;
                if (campCnt[cp]++ == 0) {
                    activePos[cp] = (u32)active.size();
                    active.push_back(cp);
                }
                hi++;
            }
            while (lo < hi && c[lo].ts < start - WEEK) {
                u32 cp = c[lo].camp;
                if (--campCnt[cp] == 0) {
                    u32 pos = activePos[cp];
                    u32 last = active.back();
                    active[pos] = last;
                    activePos[last] = pos;
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
        // reset click window state
        while (lo < hi) {
            u32 cp = c[lo].camp;
            campCnt[cp] = 0;
            lo++;
        }
        active.clear();
    }
}

// ---------------------------------------------------------------------------------------------
// Parquet writer (uncompressed, PLAIN, one data page per column chunk)
// ---------------------------------------------------------------------------------------------
struct TW {  // thrift compact protocol writer
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
    // list element structs
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
    string name;
    int ptype;
    bool optional;
    int logical;  // 0 none, 1 string, 2 date
};
struct ColData {
    string page;  // encoded page body (levels + values)
    i64 numValues;
};

static void writeParquet(const string& path, const vector<ColSpec>& cols, vector<ColData>& data, i64 numRows) {
    FILE* f = fopen(path.c_str(), "wb");
    if (!f) die("cannot write " + path);
    string out = "PAR1";
    struct Meta {
        i64 offset, size;
    };
    vector<Meta> metas;
    for (size_t ci = 0; ci < cols.size(); ci++) {
        ColData& cd = data[ci];
        TW ph;
        ph.i32f(1, 0);  // DATA_PAGE
        ph.i32f(2, (i64)cd.page.size());
        ph.i32f(3, (i64)cd.page.size());
        ph.structBegin(5);
        ph.i32f(1, numRows);
        ph.i32f(2, 0);  // PLAIN
        ph.i32f(3, 3);  // RLE
        ph.i32f(4, 3);  // RLE
        ph.structEnd();
        ph.b.push_back(0);
        Meta m{(i64)out.size(), (i64)(ph.b.size() + cd.page.size())};
        out += ph.b;
        out += cd.page;
        metas.push_back(m);
        string().swap(cd.page);
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
            fm.i64f(2, metas[ci].offset);
            fm.structBegin(3);
            fm.i32f(1, c.ptype);
            fm.listBegin(2, 5, 2);
            fm.listI32(0);
            fm.listI32(3);
            fm.listBegin(3, 8, 1);
            fm.listStr(c.name);
            fm.i32f(4, 0);
            fm.i64f(5, data[ci].numValues);
            fm.i64f(6, metas[ci].size);
            fm.i64f(7, metas[ci].size);
            fm.i64f(9, metas[ci].offset);
            fm.structEnd();
            fm.elemEnd();
            total += metas[ci].size;
        }
        fm.i64f(2, total);
        fm.i64f(3, numRows);
        fm.elemEnd();
    }
    fm.strf(6, "shopping-sessions-native");
    fm.b.push_back(0);
    if (numRows == 0) {
        // no row groups: drop the column chunk bodies we wrote
        out.resize(4);
    }
    out += fm.b;
    u32 len = (u32)fm.b.size();
    out.append((const char*)&len, 4);
    out += "PAR1";
    if (fwrite(out.data(), 1, out.size(), f) != out.size()) die("write failed " + path);
    fclose(f);
}

struct PageBuilder {
    string s;
    void i32(int32_t v) { s.append((const char*)&v, 4); }
    void i64v(i64 v) { s.append((const char*)&v, 8); }
    void str(const string& v) {
        u32 n = (u32)v.size();
        s.append((const char*)&n, 4);
        s += v;
    }
};
// definition levels (bit width 1) as one bit-packed run, prefixed with its byte length
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
    string out((const char*)&len, 4);
    return out + enc;
}

static void writeUserFile(const string& path, const vector<URow>& rows) {
    vector<ColSpec> cols = {{"user_id", P_BYTES, false, 1},  {"day", P_INT32, false, 2},
                            {"sessions", P_INT64, false, 0}, {"events", P_INT64, false, 0},
                            {"revenue_cents", P_INT64, false, 0}, {"top_category", P_BYTES, true, 1}};
    vector<ColData> data(cols.size());
    PageBuilder pu, pd, ps, pe, pr, pt;
    vector<u8> def(rows.size());
    i64 nonNull = 0;
    for (size_t i = 0; i < rows.size(); i++) {
        const URow& r = rows[i];
        pu.str(keyStr(r.person));
        pd.i32(r.day);
        ps.i64v(r.sessions);
        pe.i64v(r.events);
        pr.i64v(r.revenue);
        if (r.topcat >= 0) {
            def[i] = 1;
            pt.str(g_catNames[r.topcat]);
            nonNull++;
        }
    }
    i64 n = (i64)rows.size();
    data[0] = {std::move(pu.s), n};
    data[1] = {std::move(pd.s), n};
    data[2] = {std::move(ps.s), n};
    data[3] = {std::move(pe.s), n};
    data[4] = {std::move(pr.s), n};
    data[5] = {defLevels(def) + pt.s, n};
    writeParquet(path, cols, data, n);
}

static void writeCampaignFile(const string& path) {
    vector<ColSpec> cols = {{"campaign_id", P_BYTES, false, 1},       {"channel", P_BYTES, false, 1},
                            {"day", P_INT32, false, 2},               {"sessions", P_INT64, false, 0},
                            {"purchasing_sessions", P_INT64, false, 0}, {"revenue_cents", P_INT64, false, 0},
                            {"assisted_sessions", P_INT64, false, 0}};
    PageBuilder pc, ph, pd, ps, pp, pr, pa;
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
    vector<ColData> data = {{std::move(pc.s), n}, {std::move(ph.s), n}, {std::move(pd.s), n},
                            {std::move(ps.s), n}, {std::move(pp.s), n}, {std::move(pr.s), n},
                            {std::move(pa.s), n}};
    writeParquet(path, cols, data, n);
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

    loadManifest();
    loadLookups();
    logt("lookups");
    loadLinks();
    logt("links");

    // events + clicks
    vector<string> evPaths = listCsv(DATA + "/events"), clPaths = listCsv(DATA + "/clicks");
    vector<MFile> evFiles, clFiles;
    for (auto& p : evPaths) evFiles.push_back(mapFile(p));
    for (auto& p : clPaths) clFiles.push_back(mapFile(p));
    size_t evBytes = 0, clBytes = 0;
    for (auto& f : evFiles) evBytes += f.n;
    for (auto& f : clFiles) clBytes += f.n;
    vector<Chunk> evChunks, clChunks;
    makeChunks(evFiles, 4 << 20, evChunks);
    makeChunks(clFiles, 4 << 20, clChunks);
    logt("map");

    evBuf.assign(NT, vector<vector<ERec>>(NP));
    clBuf.assign(NT, vector<vector<CRec>>(NP));
    size_t evEst = evBytes / 75 / NT / NP + 16, clEst = clBytes / 45 / NT / NP + 16;
    parallel_for(NT, [&](size_t t, int) {
        for (int p = 0; p < NP; p++) {
            evBuf[t][p].reserve(evEst + evEst / 4);
            clBuf[t][p].reserve(clEst + clEst / 4);
        }
    });
    logt("reserve");
    // one combined work list: events chunks first (bigger), then clicks
    size_t nE = evChunks.size();
    parallel_for(nE + clChunks.size(), [&](size_t i, int tid) {
        if (i < nE)
            parseEventChunk(evChunks[i], tid);
        else
            parseClickChunk(clChunks[i - nE], tid);
    });
    mergeLongs();
    logt("parse events+clicks");

    sBuf.assign(NT, vector<vector<SRec>>(NP));
    size_t sEst = evBytes / 75 / NT / NP + 16;
    parallel_for(NT, [&](size_t t, int) {
        for (int p = 0; p < NP; p++) sBuf[t][p].reserve(sEst + sEst / 4);
    });
    parallel_for(NP, [&](size_t p, int tid) { dedupPartition((int)p, tid); });
    logt("dedup");
    // free event copies
    parallel_for(NT, [&](size_t t, int) {
        for (auto& v : evBuf[t]) vector<ERec>().swap(v);
    });
    logt("free");

    uRows.assign(NT, {});
    campAgg.assign(NT, vector<CampAgg>((size_t)(NCAMP + 1) * NDAYS, CampAgg{0, 0, 0, 0}));
    scratch.assign(NT, Scratch{});
    for (auto& s : scratch) {
        s.catRev.assign(g_catNames.size(), 0);
        s.campCnt.assign(NCAMP + 1, 0);
        s.activePos.assign(NCAMP + 1, 0);
    }
    // largest partitions first
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
    return 0;
}
