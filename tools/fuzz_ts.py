#!/usr/bin/env python3
"""Fuzz the native timestamp parser (built with -DSS_TSTEST) against a Python implementation of rules.md."""
import calendar, random, re, subprocess, sys

BIN = sys.argv[1]
I64 = 2**63 - 1
ISO = re.compile(r"([0-9]{4})-([0-9]{2})-([0-9]{2})T([0-9]{2}):([0-9]{2}):([0-9]{2})(Z|([+-])([0-9]{2}):([0-9]{2}))")

def days(y, m, d):
    y -= m <= 2
    era = (y if y >= 0 else y - 399) // 400
    yoe = y - era * 400
    doy = (153 * ((m + 9) % 12) + 2) // 5 + d - 1
    return era * 146097 + yoe * 365 + yoe // 4 - yoe // 100 + doy - 719468

def ref(s):
    if s == "":
        return "BAD"
    if re.fullmatch(r"[0-9]+", s):
        v = int(s)
        return str(v if v <= I64 else I64)
    m = ISO.fullmatch(s)
    if not m:
        return "BAD"
    y, mo, d, h, mi, se = (int(x) for x in m.groups()[:6])
    if not (1 <= mo <= 12) or d < 1 or h > 23 or mi > 59 or se > 59:
        return "BAD"
    if d > calendar.monthrange(y, mo)[1] if y >= 1 else d > [31,28,31,30,31,30,31,31,30,31,30,31][mo-1] + (mo == 2 and y % 400 == 0):
        return "BAD"
    t = days(y, mo, d) * 86400000 + h * 3600000 + mi * 60000 + se * 1000
    if m.group(7) != "Z":
        oh, om = int(m.group(9)), int(m.group(10))
        if oh > 23 or om > 59:
            return "BAD"
        off = (oh * 60 + om) * 60000
        t -= off if m.group(8) == "+" else -off
    return str(t)

rnd = random.Random(12345)
pool = list("0123456789") * 6 + list("-:TZtz+ .eE") + ["٢", "２", "é", "\x7f", "\x80"]
cases = []
for _ in range(300000):
    k = rnd.random()
    y = rnd.choice([0, 1, 1900, 2000, 2024, 2025, 2026, 2027, 2028, 2100, 2400, 9999])
    base = "%04d-%02d-%02dT%02d:%02d:%02d" % (y, rnd.randint(0, 13), rnd.randint(0, 32), rnd.randint(0, 25), rnd.randint(0, 61), rnd.randint(0, 61))
    if k < 0.35:
        s = base + rnd.choice(["Z", "z", "", "+05:30", "-10:00", "+23:59", "-00:00", "+24:00", "+05:60", "+0200", "Z ", "+5:30"])
    elif k < 0.55:
        s = "%04d-02-%02dT12:00:00Z" % (y, rnd.choice([28, 29, 30]))
    elif k < 0.75:
        s = rnd.choice(["", "0" * rnd.randint(0, 8)]) + str(rnd.randint(0, 10**rnd.randint(1, 22)))
    else:
        s = list(base + rnd.choice(["Z", "+01:00"]))
        for _ in range(rnd.randint(1, 3)):
            s[rnd.randrange(len(s))] = rnd.choice(pool)
        s = "".join(s)
    cases.append(s)
inp = "\n".join(cases) + "\n"
out = subprocess.run([BIN], input=inp.encode("utf-8"), capture_output=True).stdout.decode().split("\n")
bad = 0
for s, got in zip(cases, out):
    exp = ref(s)
    if exp != got:
        bad += 1
        if bad <= 10:
            print("MISMATCH", repr(s), "expected", exp, "got", got)
print(f"{len(cases)} cases, {bad} mismatches")
sys.exit(1 if bad else 0)
