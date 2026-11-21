#!/bin/sh
# Inside dev container: ab.sh DATASET RUNS BIN_A BIN_B  -> interleaved runs, min/median wall per binary
D=$1; R=$2; A=$3; B=$4
cat $D/*/* > /dev/null
: > /tmp/ta; : > /tmp/tb
for i in $(seq $R); do
  for X in A B; do
    if [ $X = A ]; then BIN=$A; F=/tmp/ta; else BIN=$B; F=/tmp/tb; fi
    rm -rf /tmp/o; s=$(date +%s%N); NCPU=8 $BIN $D /tmp/o; e=$(date +%s%N)
    echo $(( (e - s) / 1000000 )) >> $F
  done
done
for X in A B; do
  if [ $X = A ]; then F=/tmp/ta; N=$A; else F=/tmp/tb; N=$B; fi
  sort -n $F | awk -v n="$N" '{a[NR]=$1} END {printf "%-28s min %5d ms  median %5d ms  (n=%d)\n", n, a[1], a[int((NR+1)/2)], NR}'
done
