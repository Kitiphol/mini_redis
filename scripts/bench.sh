#!/usr/bin/env bash
# Benchmark micro-redis against real Redis (all traffic on database 0).
#
#   bash scripts/bench.sh | tee bench-results.txt
#   ROUNDS=20 THREADS="1 4 8" CLIENTS=200 bash scripts/bench.sh
#
# Needs: Rust (cargo), redis-server, redis-benchmark and perl on the PATH.
#   macOS:          brew install redis          (perl is built in)
#   Ubuntu/Debian:  sudo apt install redis-server redis-tools
# With CLIENTS above 128 on macOS, first run: sudo sysctl -w kern.ipc.somaxconn=1024
#
# Each round sends every server three workloads:
#   many  = N_SINGLE SETs over CLIENTS connections, one at a time per connection
#   pipe  = N_PIPE SETs over CLIENTS connections, 16 in flight per connection
#   one   = N_ONE SETs over ONE connection (pure round-trip latency)
# Requests per second are timed by this script with a precise clock (not
# redis-benchmark's own figure). "srv cores" is how many CPU cores the server
# kept busy during the pipelined run: if it is well below the thread count
# while every server scores the same, the benchmark client is the bottleneck.
set -euo pipefail

ROUNDS=${ROUNDS:-100}
THREADS=${THREADS:-"1 2 4 8 16"}
CLIENTS=${CLIENTS:-200}
N_SINGLE=${N_SINGLE:-500000}
N_PIPE=${N_PIPE:-1000000}
N_ONE=${N_ONE:-100000}
PORT_MICRO=7301 PORT_REDIS=7303
# CLIENT_THREADS=4 runs redis-benchmark with --threads 4. Off by default:
# with --threads, redis-benchmark only notices a run has ended on a 250 ms
# tick, which rounds every timing up to a quarter second.

cd "$(dirname "$0")/.."
for tool in redis-server redis-benchmark redis-cli perl; do
  command -v $tool >/dev/null || { echo "$tool not found (see the top of this script)"; exit 1; }
done

cores=$(nproc 2>/dev/null || sysctl -n hw.ncpu)
client_threads=${CLIENT_THREADS:-}
threads_flag=""
[ -n "$client_threads" ] && threads_flag="--threads $client_threads"

echo "Building micro-redis (release)..."
cargo build --release -q
BIN=target/release/micro-redis

pids=()
cleanup() { for p in "${pids[@]:-}"; do kill "$p" 2>/dev/null || true; done; }
trap cleanup EXIT

now() { perl -MTime::HiRes=time -e 'printf "%.6f\n", time'; }
cpu_seconds() { # pid -> CPU seconds used so far (user + system)
  if [ -r "/proc/$1/stat" ]; then
    awk -v hz="$(getconf CLK_TCK)" '{printf "%.3f\n", ($14+$15)/hz}' "/proc/$1/stat"
  else # macOS: "[hh:]mm:ss.cc"
    ps -o time= -p "$1" | awk -F: '{s=0; for(i=1;i<=NF;i++) s=s*60+$i; printf "%.3f\n", s}'
  fi
}
wait_for() { for _ in $(seq 1 50); do redis-cli -p "$1" PING >/dev/null 2>&1 && return 0; sleep 0.1; done; echo "server on $1 did not start"; exit 1; }

run() { # port pid n clients extra... -> "rps cores"
  local port=$1 pid=$2 n=$3 c=$4; shift 4
  local t0 c0 t1 c1
  c0=$(cpu_seconds "$pid"); t0=$(now)
  redis-benchmark -p "$port" -c "$c" $threads_flag -t set -n "$n" -q "$@" >/dev/null 2>&1
  t1=$(now); c1=$(cpu_seconds "$pid")
  awk -v n="$n" -v t0="$t0" -v t1="$t1" -v c0="$c0" -v c1="$c1" \
    'BEGIN {w=t1-t0; printf "%d %.2f\n", n/w, (c1-c0)/w}'
}

summary() { # file column -> "median (p25-p75)"; column 1 = rps, 2 = cores
  awk -v col="$2" '{print $col}' "$1" | sort -n | awk -v col="$2" '{v[NR]=$1} END {
    m=(NR%2)?v[(NR+1)/2]:(v[NR/2]+v[NR/2+1])/2; q1=v[int((NR-1)*0.25)+1]; q3=v[int((NR-1)*0.75)+1];
    if (col==1) printf "%5.0fk (%.0f-%.0fk)", m/1000, q1/1000, q3/1000; else printf "%.2f", m }'
}

tmp=$(mktemp -d)
echo "Machine: $cores cores. Benchmark client: $CLIENTS connections${threads_flag:+, $client_threads threads}."
echo "Each round, per server: $N_SINGLE SETs one at a time, $N_PIPE pipelined 16 deep, $N_ONE over a single connection."
echo

redis-server --port $PORT_REDIS --save "" --appendonly no --daemonize no >/dev/null 2>&1 &
redis_pid=$!; pids+=($redis_pid); wait_for $PORT_REDIS

printf "%-10s %-6s | %-20s | %-20s | %-9s | %-20s\n" "threads" "server" "many clients" "pipelined 16" "srv cores" "1 client"
for t in $THREADS; do
  TOKIO_WORKER_THREADS=$t RUST_LOG=error $BIN --bind 127.0.0.1 --port $PORT_MICRO >/dev/null 2>&1 &
  micro_pid=$!; pids+=($micro_pid)
  wait_for $PORT_MICRO

  for name in micro redis; do : > "$tmp/$name.many"; : > "$tmp/$name.pipe"; : > "$tmp/$name.one"; done
  for r in $(seq 1 "$ROUNDS"); do
    for name in micro redis; do
      case $name in
        micro) p=$PORT_MICRO; pid=$micro_pid;;
        redis) p=$PORT_REDIS; pid=$redis_pid;;
      esac
      run $p $pid "$N_SINGLE" "$CLIENTS" >> "$tmp/$name.many"
      run $p $pid "$N_PIPE" "$CLIENTS" -P 16 >> "$tmp/$name.pipe"
      run $p $pid "$N_ONE" 1 >> "$tmp/$name.one"
    done
    printf "\r  %s threads: round %d/%d" "$t" "$r" "$ROUNDS" >&2
  done
  printf "\r%60s\r" "" >&2
  for name in micro redis; do
    label=$t; [ $name = redis ] && label="1 (always)"
    printf "%-10s %-6s | %-20s | %-20s | %-9s | %-20s\n" "$label" "$name" \
      "$(summary "$tmp/$name.many" 1)" "$(summary "$tmp/$name.pipe" 1)" \
      "$(summary "$tmp/$name.pipe" 2)" "$(summary "$tmp/$name.one" 1)"
  done
  kill $micro_pid 2>/dev/null || true; wait $micro_pid 2>/dev/null || true
  sleep 0.5
done
rm -rf "$tmp"
