// Fuzz the fused parser pieces against the exact scalar code they replace.
//   g++ -O2 -march=x86-64-v3 -std=c++17 -pthread -o fuzz bench/fuzz.cpp && ./fuzz [ITER]
// 1. scanLines2<N> vs splitLine<N> per line (random CSV-ish bytes, quotes, CR, empty and long
//    lines, lines ending at the readable-memory limit, missing final newline)
// 2. cid64f vs cid64
// 3. parseTsF (SIMD F1 fast path) vs parseTs
#define SS_NOMAIN
#include "../src/ss.cpp"

static u64 rng = 0x9E3779B97F4A7C15ULL;
static inline u64 rnd() {
    rng ^= rng << 13;
    rng ^= rng >> 7;
    rng ^= rng << 17;
    return rng;
}

template <int N>
static long scanCase(long iters) {
    const char alpha[] = "ab,,\"\"\"x \r1-Z_,\"";
    long bad = 0;
    vector<char> buf;
    for (long it = 0; it < iters; it++) {
        // build a buffer of lines
        string text;
        int nl = 1 + (int)(rnd() % 6);
        for (int l = 0; l < nl; l++) {
            int kind = (int)(rnd() % 10);
            size_t len = kind == 0 ? 0 : kind == 1 ? 100 + rnd() % 200 : rnd() % 60;  // empty / long / normal
            if (kind >= 5) {  // well-formed-ish: N fields, some quoted
                string line;
                for (int f = 0; f < N; f++) {
                    if (f) line += ',';
                    string v;
                    size_t fl = rnd() % (kind == 9 ? 40 : 12);
                    for (size_t i = 0; i < fl; i++) v += "abcXYZ019-_ \r"[rnd() % 13];
                    if (rnd() % 3 == 0) v = "\"" + v + "\"";
                    line += v;
                }
                text += line;
            } else {
                for (size_t i = 0; i < len; i++) text += alpha[rnd() % (sizeof(alpha) - 1)];
            }
            if (l + 1 < nl || rnd() % 2) text += '\n';
        }
        // place it so it ends exactly at the readable limit (worst case for the 128-byte loads)
        buf.assign(text.begin(), text.end());
        const char* s = buf.data();
        const char* e = s + buf.size();
        // expected: split each line with the scalar splitter
        vector<vector<string>> want, got;
        const char* p = s;
        while (p < e) {
            const char* q = (const char*)memchr(p, '\n', (size_t)(e - p));
            const char* le = q ? q : e;
            const char* fp[N];
            u32 fl[N];
            if (splitLine<N>(p, le, fp, fl)) {
                vector<string> row;
                for (int i = 0; i < N; i++) row.emplace_back(fp[i], fl[i]);
                want.push_back(row);
            }
            p = q ? q + 1 : e;
        }
        scanLines2<N>(s, e, e, [&](const char** fp, const u32* fl) {
            vector<string> row;
            for (int i = 0; i < N; i++) row.emplace_back(fp[i], fl[i]);
            got.push_back(row);
        });
        if (want != got && ++bad <= 3) printf("scan<%d> mismatch on: [%s]\n", N, text.c_str());
    }
    return bad;
}

int main(int argc, char** argv) {
    long iters = argc > 1 ? atol(argv[1]) : 300000;
    initMasks();
    initCompact();
    long b2 = scanCase<2>(iters), b4 = scanCase<4>(iters), b7 = scanCase<7>(iters);
    printf("scanLines2 vs splitLine: N=2 %ld, N=4 %ld, N=7 %ld mismatches (%ld buffers each)\n", b2, b4, b7, iters);

    const char pool[] = "-0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ_abcdefghijklmnopqrstuvwxyz,./@[`{ \x7f\x80\xff\t";
    vector<char> buf(64);
    long cbad = 0;
    for (long it = 0; it < 3 * iters; it++) {
        size_t len = 1 + rnd() % 12, at = rnd() % 52;
        for (auto& c : buf) c = pool[rnd() % (sizeof(pool) - 1)];
        if (rnd() % 4) for (size_t i = 0; i < len; i++) buf[at + i] = pool[rnd() % 64];
        if (cid64(buf.data() + at, len) != cid64f(buf.data() + at, len, buf.data() + 64)) cbad++;
    }
    printf("cid64f vs cid64: %ld cases, %ld mismatches\n", 3 * iters, cbad);

    // timestamps: mostly near-valid F1/F2, with single-byte corruptions
    long tbad = 0, tn = 0;
    char ts[40];
    for (long it = 0; it < 3 * iters; it++) {
        int y = (int)(rnd() % 10000), mo = (int)(rnd() % 14), d = (int)(rnd() % 33), h = (int)(rnd() % 26),
            mi = (int)(rnd() % 62), se = (int)(rnd() % 62);
        if (rnd() % 2) { y = 1999 + (int)(rnd() % 30); mo = 1 + (int)(rnd() % 12); d = 1 + (int)(rnd() % 28); h %= 24; mi %= 60; se %= 60; }
        int n = snprintf(ts, sizeof ts, "%04d-%02d-%02dT%02d:%02d:%02dZ", y, mo, d, h, mi, se);
        if (rnd() % 4 == 0) n = snprintf(ts, sizeof ts, "%04d-%02d-%02dT%02d:%02d:%02d%c%02d:%02d", y, mo, d, h, mi, se,
                                        rnd() % 2 ? '+' : '-', (int)(rnd() % 25), (int)(rnd() % 61));
        if (rnd() % 3 == 0) ts[rnd() % n] = "0123456789-T:Zz+ /\xff"[rnd() % 19];  // corrupt one byte
        if (n > 0 && n < 32) {
            char tb[64] = {0};
            memcpy(tb, ts, (size_t)n);
            i64 a = 0, b = 0;
            bool ra = parseTs(tb, (u32)n, a), rb = parseTsF(tb, (u32)n, b);
            tn++;
            if (ra != rb || (ra && a != b)) {
                if (++tbad <= 3) printf("ts mismatch: %.*s\n", n, tb);
            }
        }
    }
    printf("parseTsF vs parseTs: %ld cases, %ld mismatches\n", tn, tbad);
    return (b2 | b4 | b7 | cbad | tbad) ? 1 : 0;
}
