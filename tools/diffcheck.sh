#!/bin/sh
# Inside dev container: compares build/ss against build/ss_v1 (or a stored reference) on datasets.
cd /w/starter-duckdb
rm -rf /tmp/a && /w/build/ss /vol/dev /tmp/a && echo "== dev vs official answer" && python scripts/compare.py /tmp/a /w/dev-answer | tail -1
for d in s7 s42 s2027; do
  rm -rf /tmp/a /tmp/b; /w/build/ss /vol/$d /tmp/a; /w/build/ss_v1 /vol/$d /tmp/b
  echo "== $d v2 vs v1"; python scripts/compare.py /tmp/a /tmp/b | tail -1
done
rm -rf /tmp/a; /w/build/ss /vol/p20 /tmp/a; echo "== p20 v2 vs duckdb"; python scripts/compare.py /tmp/a /vol/ref_p20
