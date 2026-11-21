// Fused event-line parser prototype: line masks -> field positions -> decoded record, with a
// SIMD fast path for F1 timestamps. Anything unusual falls back to the exact production code.
#pragma once

// (cid64f, tsF1 and parseTsF come from src/ss.cpp)

template <class F>
static inline void fusedEvents(const char* s, const char* e, const char* lim, F&& emit) {
    alignas(64) char tmp[128];
    const char* fp[7];
    u32 fl[7];
    Out it;
    while (s < e) {
        const char* ls = s;
        LineM m;
        if (!lineMasks(s, e, lim, tmp, m)) {  // long line: exact scalar path
            const char* nlp = (const char*)memchr(s, '\n', (size_t)(e - s));
            const char* le = nlp ? nlp : e;
            if (splitLine<7>(s, le, fp, fl) && fl[0]) {
                decodeCur(fp, fl, lim, it);
                emit(it);
            }
            s = nlp ? nlp + 1 : e;
            continue;
        }
        s = m.next;
        if (m.L == 0 || popc128(m.C) != 6) continue;
        u128 Qs;
        if (!quotesOk(m, Qs)) continue;
        // field i = [a_i + q_i, b_i - q_i)
#ifdef BRANCHLESS_FIELDS
        // branchless comma positions: tzcnt of the low word, else 64 + tzcnt of the high word
        u64 lo = (u64)m.C, hi = (u64)(m.C >> 64);
        u64 qlo = (u64)Qs, qhi = (u64)(Qs >> 64);
        u32 a = 0;
        for (int i = 0; i < 7; i++) {
            u32 b;
            if (i < 6) {
                u32 tl = (u32)_tzcnt_u64(lo), th = 64 + (u32)_tzcnt_u64(hi);
                b = lo ? tl : th;
                u64 nlo = _blsr_u64(lo), nhi = _blsr_u64(hi);
                hi = lo ? hi : nhi;
                lo = nlo;
            } else {
                b = m.L;
            }
            u32 q = (u32)((a < 64 ? qlo >> (a & 63) : qhi >> (a & 63)) & 1);
            fp[i] = ls + a + q;
            fl[i] = b - a - 2 * q;
            a = b + 1;
        }
#else
        u128 C = m.C;
        u32 a = 0;
        for (int i = 0; i < 7; i++) {
            u32 b;
            if (i < 6) {
                b = (u32)ctz128(C);
                C &= C - 1;
            } else {
                b = m.L;
            }
            u32 q = (u32)(Qs >> a) & 1;
            fp[i] = ls + a + q;
            fl[i] = b - a - 2 * q;
            a = b + 1;
        }
#endif
        if (fl[0] == 0) continue;
        ERec& r = it.r;
        r.eid = cid64f(fp[0], fl[0], lim);
        i64 ing;
        if (parseTsF(fp[3], fl[3], ing)) {
            i64 sec = fdiv(ing, 1000) - CWS / 1000;
            r.ing = sec > INT32_MAX ? INT32_MAX : sec < INT32_MIN ? INT32_MIN : (i32)sec;
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
        if (fl[1] != 0 && fl[5] != 0 && parseTsF(fp[2], fl[2], ts) && ts >= WS && ts < WE) {
            int typ = -1;
            if (fl[4] == 4) {
                u32 x;
                memcpy(&x, fp[4], 4);
                if (x == 0x77656976u /* view */ || x == 0x74726163u /* cart */) typ = 0;
            } else if (fl[4] == 8) {
                u64 x;
                memcpy(&x, fp[4], 8);
                if (x == 0x6573616863727570ull /* purchase */) typ = 1;
            }
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
        emit(it);
    }
}
