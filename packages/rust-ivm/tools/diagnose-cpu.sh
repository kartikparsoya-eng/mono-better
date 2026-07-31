#!/usr/bin/env bash
# diagnose-cpu.sh — why is only half the machine busy?
#
# Run INSIDE the container (not on the host). Answers, in order:
#   A. how many CPUs is this container actually allowed?
#   B. is it being throttled?
#   C. are "32 cores" really 32, or 16 physical x SMT?
#   D. are threads runnable-but-waiting (CPU-starved) or blocked (I/O / lock)?
#   E. what does Node think, and how many workers did it spawn?
#
# The distinction that matters: idle cores with a LOW run queue means work is
# BLOCKED (I/O, or a connection-pool cap). Idle cores with a HIGH run queue and
# nonzero throttling means you are QUOTA-LIMITED. Those have opposite fixes, and
# no amount of IVM optimisation helps either one.

set -uo pipefail
say() { printf '\n=== %s ===\n' "$1"; }

say "A. cgroup CPU allocation"
if [[ -f /sys/fs/cgroup/cpu.max ]]; then
  read -r quota period < /sys/fs/cgroup/cpu.max
  if [[ "$quota" == "max" ]]; then
    echo "cpu.max = max (UNLIMITED) — not quota-limited"
  else
    echo "cpu.max = $quota $period  ->  $((quota / period)) CPUs allowed"
    echo ">>> If this is 16, the mystery is solved: you are capped at 16."
  fi
elif [[ -f /sys/fs/cgroup/cpu/cpu.cfs_quota_us ]]; then
  q=$(cat /sys/fs/cgroup/cpu/cpu.cfs_quota_us)
  p=$(cat /sys/fs/cgroup/cpu/cpu.cfs_period_us)
  if [[ "$q" == "-1" ]]; then echo "cfs_quota = -1 (UNLIMITED)"; else
    echo "cfs_quota=$q cfs_period=$p  ->  $((q / p)) CPUs allowed"
  fi
else
  echo "no cgroup cpu controller visible (not containerised?)"
fi
echo "cpuset (which cores are permitted):"
cat /sys/fs/cgroup/cpuset.cpus.effective 2>/dev/null \
  || cat /sys/fs/cgroup/cpuset/cpuset.effective_cpus 2>/dev/null \
  || echo "  (none — all cores permitted)"

say "B. throttling — the decisive number"
if [[ -f /sys/fs/cgroup/cpu.stat ]]; then
  grep -E 'nr_periods|nr_throttled|throttled_usec' /sys/fs/cgroup/cpu.stat
  echo ">>> nr_throttled climbing over time = you ARE quota-limited."
  echo ">>> nr_throttled flat at 0        = quota is NOT the ceiling."
else
  echo "cpu.stat unavailable"
fi

say "C. physical cores vs SMT threads"
lscpu 2>/dev/null | grep -E '^CPU\(s\):|Thread\(s\) per core|Core\(s\) per socket|Socket\(s\)' \
  || echo "lscpu unavailable"
echo ">>> If 'Thread(s) per core: 2' and you see 16 busy, those may be 16"
echo ">>> PHYSICAL cores fully saturated — i.e. you are NOT underusing the box."

say "D. run queue — starved or blocked?"
if command -v vmstat >/dev/null; then
  echo "columns: r=runnable  b=blocked  us/sy=cpu%  wa=iowait"
  vmstat 1 5
  echo ">>> r >> allowed-CPUs        -> CPU-starved / throttled"
  echo ">>> r low AND cores idle     -> BLOCKED (I/O or a pool cap), not CPU-bound"
  echo ">>> high wa                  -> I/O bound -> RUST_IVM_READ_LANES helps"
else
  echo "vmstat unavailable; try:  cat /proc/loadavg  and  cat /proc/pressure/cpu"
  cat /proc/loadavg 2>/dev/null
  echo "PSI (pressure stall):"
  cat /proc/pressure/cpu 2>/dev/null || echo "  (PSI unavailable)"
  cat /proc/pressure/io  2>/dev/null
fi

say "E. what Node believes, and how many workers exist"
node -e 'const os=require("os");console.log("availableParallelism:",os.availableParallelism(),"| os.cpus().length:",os.cpus().length)' 2>/dev/null \
  || echo "node unavailable"
echo "ZERO_NUM_SYNC_WORKERS=${ZERO_NUM_SYNC_WORKERS:-<unset -> derived from availableParallelism()-1>}"
echo "UV_THREADPOOL_SIZE=${UV_THREADPOOL_SIZE:-<unset, default 4>}"
echo "RUST_IVM_READ_LANES=${RUST_IVM_READ_LANES:-<unset -> 0, OFF>}"
echo "RUST_IVM_CG_WORKERS=${RUST_IVM_CG_WORKERS:-<unset -> thread-per-client-group>}"
echo "ZERO_UPSTREAM_MAX_CONNS=${ZERO_UPSTREAM_MAX_CONNS:-<unset>}"
echo ">>> availableParallelism() reports HOST cpus and IGNORES the cgroup quota."
echo ">>> If it says 78 while cpu.max says 16, worker count was derived from"
echo ">>> the wrong number and nothing downstream corrects it."

say "F. thread count actually running"
ls /proc/*/task 2>/dev/null | wc -l | xargs echo "total threads in container (approx):"
pgrep -c node 2>/dev/null | xargs echo "node processes:"

cat <<'EOF'

--- DECISION TREE ---
  B shows nr_throttled climbing        -> quota-limited. Raise the CPU limit, or
                                          cut ZERO_NUM_SYNC_WORKERS to match it.
  C shows 2 threads/core, 16 busy      -> "32 cores" is SMT; you are saturated.
  D shows low r + idle cores + high wa -> I/O bound. RUST_IVM_READ_LANES=2 is the
                                          lever (measured +48% on a 13k-row hydrate).
  D shows low r + idle cores + low wa  -> BLOCKED on a lock or pool. Prime suspect:
                                          ZERO_UPSTREAM_MAX_CONNS=15 (~16 concurrent).
  E shows availableParallelism >> quota-> worker count derived from host cpus.
                                          Pin ZERO_NUM_SYNC_WORKERS explicitly.

None of these are fixed by IVM work. Establish which one first.
EOF
