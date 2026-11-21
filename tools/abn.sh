#!/bin/sh
# Inside dev container: abn.sh DATASET RUNS BIN...   interleaved runs, min/median per binary
D=$1; R=$2; shift 2
cat $D/*/* > /dev/null
i=0; for B in "$@"; do : > /tmp/t$i; i=$((i+1)); done
for r in $(seq $R); do
  i=0; for B in "$@"; do rm -rf /tmp/o; s=$(date +%s%N); NCPU=8 $B $D /tmp/o; e=$(date +%s%N); echo $(( (e-s)/1000000 )) >> /tmp/t$i; i=$((i+1)); done
done
i=0; for B in "$@"; do sort -n /tmp/t$i | awk -v n="$B" '{a[NR]=$1} END {printf "%-26s min %5d  median %5d ms\n", n, a[1], a[int((NR+1)/2)]}'; i=$((i+1)); done
