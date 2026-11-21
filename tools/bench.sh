#!/bin/sh
# Inside dev container: bench.sh BINARY DATASET RUNS
B=$1; D=$2; R=${3:-3}
cat $D/*/* > /dev/null
for i in $(seq $R); do rm -rf /tmp/o; /usr/bin/time -f 'TOTAL wall %e s  cpu %U+%S s  maxrss %M KB' env VERBOSE=1 NCPU=8 $B $D /tmp/o 2>&1 | tr '\n' '|' | sed 's/|\[/\n[/g; s/|TOTAL/\nTOTAL/'; echo; done
