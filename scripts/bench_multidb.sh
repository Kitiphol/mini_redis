#!/usr/bin/env bash
# Benchmark micro-redis across all 16 databases (pure bash, no Python).
#
#   bash scripts/bench_multidb.sh | tee bench-results.txt
#   ROUNDS=20 THREADS="1 4 8" bash scripts/bench_multidb.sh
#
# Needs: cargo, redis-server, redis-cli, perl on the PATH.
#   macOS:  brew install redis
#   Linux:  sudo apt install redis-server redis-tools
#
# How the multi-db workload works:
#   CLIENTS connections are distributed round-robin across DATABASES databases.
#   One redis-benchmark process per database, each with --dbnum <db> (it
#   sends SELECT <db> as the first command on every connection it opens).
#   All of them run in parallel; timing covers the whole parallel run.
#
# Workloads per round:
#   many  = N_SINGLE SETs, CLIENTS conns spread across DATABASES dbs, pipeline 1
#   pipe  = N_PIPE   SETs, CLIENTS conns spread across DATABASES dbs, pipeline 16
#   one   = N_ONE    SETs, 1 conn,  db 0 only (pure latency baseline)

set -euo pipefail

ROUNDS="${ROUNDS:-100}"
THREADS="${THREADS:-1 2 4 8 16}"
CLIENTS="${CLIENTS:-200}"
DATABASES="${DATABASES:-16}"

N_SINGLE="${N_SINGLE:-500000}"
N_PIPE="${N_PIPE:-1000000}"
N_ONE="${N_ONE:-100000}"

PORT_MICRO=7301
PORT_REDIS=7303

# ---------------------------------------------------------------------------
# Setup
# ---------------------------------------------------------------------------

cd "$(dirname "$0")/.."

for tool in redis-server redis-benchmark redis-cli perl; do
  command -v "$tool" >/dev/null || { echo "$tool not found"; exit 1; }
done

cores=$(nproc 2>/dev/null || sysctl -n hw.ncpu)
echo "Building micro-redis (release)..."
cargo build --release -q
BIN=target/release/micro-redis

pids=()
cleanup() { for p in "${pids[@]:-}"; do kill "$p" 2>/dev/null || true; done; }
trap cleanup EXIT

now()         { perl -MTime::HiRes=time -e 'printf "%.6f\n", time'; }
cpu_seconds() {
  if [ -r "/proc/$1/stat" ]; then
    awk -v hz="$(getconf CLK_TCK)" '{printf "%.3f\n", ($14+$15)/hz}' "/proc/$1/stat"
  else
    ps -o time= -p "$1" | awk -F: '{s=0; for(i=1;i<=NF;i++) s=s*60+$i; printf "%.3f\n", s}'
  fi
}
wait_for() {
  for _ in $(seq 1 80); do
    redis-cli -p "$1" PING >/dev/null 2>&1 && return 0
    sleep 0.1
  done
  echo "server on $1 did not start"; exit 1
}

summary() { # file col -> "median (p25-p75)" or "median" for cores
  awk -v col="$2" '{print $col}' "$1" | sort -n | awk -v col="$2" '{v[NR]=$1} END {
    m=(NR%2)?v[(NR+1)/2]:(v[NR/2]+v[NR/2+1])/2
    q1=v[int((NR-1)*0.25)+1]; q3=v[int((NR-1)*0.75)+1]
    if (col==1) printf "%5.0fk (%.0f-%.0fk)", m/1000, q1/1000, q3/1000
    else        printf "%.2f", m
  }'
}

# ---------------------------------------------------------------------------
# run_multidb port pid n_total clients pipeline -> appends "rps cores" to stdout
#
# Splits CLIENTS across DATABASES databases.
# Each database slice runs its own redis-benchmark in background.
# We time the whole parallel burst ourselves.
# ---------------------------------------------------------------------------

run_multidb() {
  local port=$1 pid=$2 n_total=$3 clients=$4 pipeline=$5
  local c0 t0 t1 c1

  # clients and requests per database (round-robin distribution)
  local c_per_db=$(( clients / DATABASES ))
  local n_per_db=$(( n_total / DATABASES ))
  # at minimum 1 client and 1 request per db
  (( c_per_db < 1 )) && c_per_db=1
  (( n_per_db < 1 )) && n_per_db=1

  c0=$(cpu_seconds "$pid")
  t0=$(now)

  local bench_pids=()
  for db in $(seq 0 $(( DATABASES - 1 ))); do
    redis-benchmark \
      -p "$port" \
      --dbnum "$db" \
      -c "$c_per_db" \
      -t set \
      -n "$n_per_db" \
      -P "$pipeline" \
      -q \
      >/dev/null 2>&1 &
    bench_pids+=($!)
  done

  # wait for all database slices to finish
  for bp in "${bench_pids[@]}"; do
    wait "$bp" 2>/dev/null || true
  done

  t1=$(now)
  c1=$(cpu_seconds "$pid")

  local n_actual=$(( n_per_db * DATABASES ))
  awk -v n="$n_actual" -v t0="$t0" -v t1="$t1" -v c0="$c0" -v c1="$c1" \
    'BEGIN { w=t1-t0; printf "%d %.2f\n", (w>0?n/w:0), (w>0?(c1-c0)/w:0) }'
}

# Single-connection baseline (db 0 only, no parallelism)
run_one() {
  local port=$1 pid=$2 n=$3
  local c0 t0 t1 c1
  c0=$(cpu_seconds "$pid"); t0=$(now)
  redis-benchmark -p "$port" --dbnum 0 -c 1 -t set -n "$n" -P 1 -q >/dev/null 2>&1
  t1=$(now); c1=$(cpu_seconds "$pid")
  awk -v n="$n" -v t0="$t0" -v t1="$t1" -v c0="$c0" -v c1="$c1" \
    'BEGIN { w=t1-t0; printf "%d %.2f\n", (w>0?n/w:0), (w>0?(c1-c0)/w:0) }'
}

# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

tmp=$(mktemp -d)

echo
echo "Machine: $cores cores"
echo "Databases: $DATABASES  |  Clients: $CLIENTS ($(( CLIENTS / DATABASES )) per db)"
echo "Rounds: $ROUNDS  |  Threads: $THREADS"
echo "N_SINGLE=$N_SINGLE  N_PIPE=$N_PIPE  N_ONE=$N_ONE"
echo

redis-server \
  --port "$PORT_REDIS" \
  --databases "$DATABASES" \
  --save "" \
  --appendonly no \
  --daemonize no \
  >/dev/null 2>&1 &
redis_pid=$!; pids+=("$redis_pid"); wait_for "$PORT_REDIS"

printf "%-10s %-6s | %-22s | %-22s | %-9s | %-20s\n" \
  "threads" "server" "many clients (16 db)" "pipelined 16 (16 db)" "srv cores" "1 client (db0)"

for t in $THREADS; do
  TOKIO_WORKER_THREADS=$t RUST_LOG=error \
    "$BIN" --bind 127.0.0.1 --port "$PORT_MICRO" >/dev/null 2>&1 &
  micro_pid=$!; pids+=("$micro_pid")
  wait_for "$PORT_MICRO"

  for name in micro redis; do
    : > "$tmp/$name.many"
    : > "$tmp/$name.pipe"
    : > "$tmp/$name.one"
  done

  for r in $(seq 1 "$ROUNDS"); do
    for name in micro redis; do
      case $name in
        micro) p=$PORT_MICRO; pid=$micro_pid;;
        redis) p=$PORT_REDIS; pid=$redis_pid;;
      esac
      run_multidb "$p" "$pid" "$N_SINGLE" "$CLIENTS" 1  >> "$tmp/$name.many"
      run_multidb "$p" "$pid" "$N_PIPE"   "$CLIENTS" 16 >> "$tmp/$name.pipe"
      run_one     "$p" "$pid" "$N_ONE"                  >> "$tmp/$name.one"
    done
    printf "\r  %s threads: round %d/%d" "$t" "$r" "$ROUNDS" >&2
  done
  printf "\r%60s\r" "" >&2

  for name in micro redis; do
    label=$t; [ "$name" = redis ] && label="n/a"
    printf "%-10s %-6s | %-22s | %-22s | %-9s | %-20s\n" \
      "$label" "$name" \
      "$(summary "$tmp/$name.many" 1)" \
      "$(summary "$tmp/$name.pipe" 1)" \
      "$(summary "$tmp/$name.pipe" 2)" \
      "$(summary "$tmp/$name.one"  1)"
  done

  kill "$micro_pid" 2>/dev/null || true
  wait "$micro_pid" 2>/dev/null || true
  sleep 0.5
done

rm -rf "$tmp"
echo
echo "Done."
