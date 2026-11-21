#!/bin/sh
# Inside dev container: repeated runs at different thread counts, each compared with a reference.
cd /w/starter-duckdb
fail=0
for th in 8 8 16 32 3; do
  rm -rf /tmp/a; THREADS=$th /w/build/ss /vol/p20 /tmp/a
  r=$(python scripts/compare.py /tmp/a /vol/ref_p20 | tail -1); echo "p20 THREADS=$th: $r"; [ "$r" = MATCH ] || fail=1
done
for d in s7 s42 s2027; do for th in 32 64; do
  rm -rf /tmp/a /tmp/b; THREADS=$th /w/build/ss /vol/$d /tmp/a; /w/build/ss_v1 /vol/$d /tmp/b
  r=$(python scripts/compare.py /tmp/a /tmp/b | tail -1); echo "$d THREADS=$th: $r"; [ "$r" = MATCH ] || fail=1
done; done
echo "STRESS $( [ $fail = 0 ] && echo PASS || echo FAIL )"
