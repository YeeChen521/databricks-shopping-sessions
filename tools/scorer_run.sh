#!/bin/sh
# Scorer-like run on Windows/Docker Desktop: scorer_run.sh DATASET_SUBDIR [RUNS] [IMAGE]
# Data comes from the ssdata volume (read-only subpath); output goes to ssdata:/runs/out.
# Mirrors scripts/docker_run.sh flags. MEM is 11g here only because the local Docker VM has 12 GB.
D=$1; R=${2:-3}; IMAGE=${3:-shopping-sessions:latest}; MEM=${MEM:-11g}; CPUS=${CPUS:-8}
export MSYS_NO_PATHCONV=1
for i in $(seq $R); do
  docker run --rm -v ssdata:/vol debian:bookworm-slim sh -c "rm -rf /vol/runs && mkdir -p /vol/runs/out /vol/runs/scratch && chmod 777 /vol/runs/out /vol/runs/scratch && cat /vol/$D/*/* /vol/$D/* > /dev/null 2>&1"
  t0=$(date +%s%N)
  docker run --rm --network none --read-only --tmpfs /tmp:rw,size=64m,mode=1777 \
    --cap-drop ALL --security-opt no-new-privileges --pids-limit 4096 \
    --cpus "$CPUS" --memory "$MEM" --memory-swap "$MEM" --user 1000:1000 \
    --mount type=volume,src=ssdata,dst=/data,volume-subpath=$D,readonly \
    --mount type=volume,src=ssdata,dst=/output,volume-subpath=runs/out \
    --mount type=volume,src=ssdata,dst=/scratch,volume-subpath=runs/scratch \
    -e DATA_DIR=/data -e OUTPUT_DIR=/output -e SCRATCH_DIR=/scratch -e HOME=/scratch \
    -e NCPU="$CPUS" -e MEM_GB="${MEM%g}" ${EXTRA_ENV:+-e $EXTRA_ENV} "$IMAGE"
  rc=$?
  t1=$(date +%s%N)
  echo "run $i: exit $rc  container wall $(( (t1 - t0) / 1000000 )) ms"
done
