#!/usr/bin/env python3
"""Writes random CSV-ish files for the scanner test driver (built with -DSS_SCANTEST)."""
import random, sys
rnd = random.Random(int(sys.argv[2]))
atoms = ['a', 'bc', 'k1', '', '"', '""', '"x"', ',', '\r', ' ', 'u', '"ab', 'ab"', 'é', '\t', '0123456789' * rnd.randint(1, 3)]
lines = []
for _ in range(200000):
    k = rnd.random()
    if k < 0.6:  # mostly well-formed 4-field lines, random quoting
        f = [rnd.choice(['k1', 'uabc', 'c_12', '1791071890000', '', 'x' * rnd.randint(0, 60)]) for _ in range(4)]
        f = ['"%s"' % x if rnd.random() < 0.3 else x for x in f]
        line = ','.join(f)
    else:
        line = ''.join(rnd.choice(atoms) for _ in range(rnd.randint(0, 40)))
    lines.append(line)
data = '\n'.join(lines)
if rnd.random() < 0.5:
    data += '\n'
open(sys.argv[1], 'w', encoding='utf-8', newline='').write(data)
