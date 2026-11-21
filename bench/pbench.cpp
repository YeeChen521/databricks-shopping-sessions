// Isolated single-thread benchmark of event-line parsing. Includes the production source and
// compares scanners and decoders on identical in-memory data.
//   g++ -O3 -march=x86-64-v3 -std=c++17 -pthread -o pbench bench/pbench.cpp
//   ./pbench DATA_DIR NFILES [REPS]
#define SS_NOMAIN
#include "../src/ss.cpp"
#include <chrono>

// ---- line-at-a-time scanner (same contract as scanLines; from the later production code) ----
// (line scanner, cid64f and parseTsF come from src/ss.cpp)

// ---- the production decode (parseEventChunk's line callback, minus table lookups/pushes) ----
struct Out {
    ERec r;
    Key uid, pid;
    u64 hu, hp;
};
static u64 g_sink;
static inline void sinkOut(const Out& o) {
    g_sink += o.r.eid ^ (u64)o.r.ing ^ o.r.ts ^ o.r.price ^ o.r.cat ^ o.uid.a ^ o.uid.b ^ o.pid.a ^ o.pid.b ^ o.hu ^ o.hp;
}
static inline void decodeCur(const char** fp, const u32* fl, const char* lim, Out& it) {
    ERec& r = it.r;
    r.eid = cid64(fp[0], fl[0]);
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
    it.uid = Key{0, 0};
    it.pid = Key{0, 0};
    it.hu = it.hp = 0;
    i64 ts;
    if (fl[1] != 0 && fl[5] != 0 && parseTs(fp[2], fl[2], ts) && ts >= WS && ts < WE) {
        int typ = -1;
        if (fl[4] == 4) {
            if (memcmp(fp[4], "view", 4) == 0 || memcmp(fp[4], "cart", 4) == 0) typ = 0;
        } else if (fl[4] == 8 && memcmp(fp[4], "purchase", 8) == 0)
            typ = 1;
        u32 price = 0;
        if (typ >= 0 && (typ == 0 || parsePrice(fp[6], fl[6], price)) && !(ing - ts > DAY)) {
            r.cat = 0;
            r.ts = (u32)(ts - CWS);
            r.price = price;
            it.uid = packKey<false>(fp[1], fl[1], lim, 0);
            it.pid = packKey<false>(fp[5], fl[5], lim, 0);
            it.hu = hk(it.uid);
            it.hp = hk(it.pid);
        }
    }
}

#include "fused.h"
static bool eqOut(const Out& x, const Out& y) {
    return x.r.eid == y.r.eid && x.r.pid == y.r.pid && x.r.ts == y.r.ts && x.r.ing == y.r.ing && x.r.price == y.r.price &&
           x.r.cat == y.r.cat && x.uid == y.uid && x.pid == y.pid && x.hu == y.hu && x.hp == y.hp;
}

// ---------------------------------------------------------------------------------------------
struct File {
    vector<char> buf;
    const char *s, *e, *lim;
};
static File loadFile(const string& path) {
    File f;
    FILE* fp = fopen(path.c_str(), "rb");
    if (!fp) die("open " + path);
    fseek(fp, 0, SEEK_END);
    size_t n = (size_t)ftell(fp);
    fseek(fp, 0, SEEK_SET);
    f.buf.assign(n + 256, 0);
    if (fread(f.buf.data(), 1, n, fp) != n) die("read");
    fclose(fp);
    const char* p = f.buf.data();
    const char* h = (const char*)memchr(p, '\n', n);
    f.s = h ? h + 1 : p + n;
    f.e = p + n;
    f.lim = p + n;  // like production: readable memory ends at the file end
    return f;
}

int main(int argc, char** argv) {
    if (argc < 3) die("usage: pbench DATA_DIR NFILES [REPS]");
    DATA = argv[1];
    int nf = atoi(argv[2]);
    int reps = argc > 3 ? atoi(argv[3]) : 5;
    NT = 1;
    g_arena.init(1ull << 30);
    g_longs.assign(1, {});
    g_slabs.assign(1, Slab{});
    initMasks();
    initCompact();
    loadManifest();
    loadLookups();
    vector<File> files;
    size_t bytes = 0;
    for (int i = 0; i < nf; i++) {
        char name[64];
        snprintf(name, sizeof name, "/events/part-%05d.csv", i);
        files.push_back(loadFile(DATA + name));
        bytes += (size_t)(files.back().e - files.back().s);
    }
    u64 rows = 0;
    for (auto& f : files) scanLines<7>(f.s, f.e, [&](const char**, const u32*) { rows++; });
    printf("%d files, %.1f MB, %llu well-formed rows\n", nf, bytes / 1e6, (unsigned long long)rows);

    auto timeit = [&](const char* name, auto&& body) {
        double best = 1e9;
        u64 sink0 = g_sink;
        for (int r = 0; r < reps; r++) {
            auto t0 = chrono::steady_clock::now();
            for (auto& f : files) body(f);
            double t = chrono::duration<double>(chrono::steady_clock::now() - t0).count();
            best = min(best, t);
        }
        printf("%-34s %7.1f ms  %6.2f ns/row  (sink %llx)\n", name, best * 1e3, best * 1e9 / rows,
               (unsigned long long)(g_sink - sink0));
    };
    timeit("scan: block scanner (6e055ee)", [&](File& f) {
        scanLines<7>(f.s, f.e, [&](const char** fp, const u32* fl) { g_sink += fl[0] + fl[6] + (u64)fp[3]; });
    });
    timeit("scan: line scanner", [&](File& f) {
        scanLines2<7>(f.s, f.e, f.lim, [&](const char** fp, const u32* fl) { g_sink += fl[0] + fl[6] + (u64)fp[3]; });
    });
    timeit("decode: block scanner + current", [&](File& f) {
        Out o;
        scanLines<7>(f.s, f.e, [&](const char** fp, const u32* fl) {
            if (fl[0] == 0) return;
            decodeCur(fp, fl, f.lim, o);
            sinkOut(o);
        });
    });
    timeit("decode: line scanner + current", [&](File& f) {
        Out o;
        scanLines2<7>(f.s, f.e, f.lim, [&](const char** fp, const u32* fl) {
            if (fl[0] == 0) return;
            decodeCur(fp, fl, f.lim, o);
            sinkOut(o);
        });
    });
    timeit("decode: fused", [&](File& f) {
        Out o;
        fusedEvents(f.s, f.e, f.lim, [&](const Out& x) { sinkOut(x); });
    });
    // equivalence: fused vs current on every line
    {
        vector<Out> a, b;
        u64 diff = 0;
        for (auto& f : files) {
            a.clear();
            b.clear();
            scanLines<7>(f.s, f.e, [&](const char** fp, const u32* fl) {
                if (fl[0] == 0) return;
                Out o;
                decodeCur(fp, fl, f.lim, o);
                a.push_back(o);
            });
            fusedEvents(f.s, f.e, f.lim, [&](const Out& x) { b.push_back(x); });
            if (a.size() != b.size()) {
                printf("row count differs: %zu vs %zu\n", a.size(), b.size());
                diff++;
                continue;
            }
            for (size_t i = 0; i < a.size(); i++)
                if (!eqOut(a[i], b[i]) && ++diff < 5) printf("row %zu differs\n", i);
        }
        printf("fused vs current: %llu differences\n", (unsigned long long)diff);
    }
    return 0;
}
