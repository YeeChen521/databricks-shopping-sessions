#!/bin/sh
# Usage (inside dev container): crosscheck.sh DATASET...   compares native vs DuckDB reference
set -e
for d in "$@"; do
  n=$(basename $d)
  rm -rf /tmp/n_$n /tmp/r_$n
  /w/build/ss $d /tmp/n_$n
  SCRATCH_DIR=/tmp python /w/reference/duckdb_pipeline.py $d /tmp/r_$n
  echo "== $n"; (cd /w/starter-duckdb && python scripts/compare.py /tmp/n_$n /tmp/r_$n)
done
