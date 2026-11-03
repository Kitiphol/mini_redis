# micro-redis

A Redis-compatible server written in Rust on top of `tokio`, for **GOMS Mini 2: Micro-Redis**.

- Speaks **RESP2**, so `redis-cli`, `redis-benchmark` and Redis client libraries work with it.
- Supports the required `SELECT`, `GET`, `SET`, `PING`, `EXISTS`, `LPUSH`, `RPUSH`, `BLPOP` and `BRPOP`.
- **Runs the Mini 1 distributed crawler (extra credit).** That needs about 70 more commands: strings with expiry, lists, sets, hashes, `KEYS`, `MULTI`/`EXEC`, and **Lua scripting** (`EVAL`, `EVALSHA`, `SCRIPT LOAD`). The crawler's full end-to-end suite passes against micro-redis with the same results as real Redis. See [Extra credit](#extra-credit-running-the-distributed-crawler).
- Keeps **16 independent databases**, numbered 0–15.
- Is **multithreaded and asynchronous**: one tokio task per connection on a multi-thread runtime.
- Is **linearizable**, including the blocking pops. The argument is in [How the service stays linearizable](#how-the-service-stays-linearizable).
- Uses **one mutex per database**. Different databases run in parallel; commands on the same database take turns. See [Performance and design choices](#performance-and-design-choices).
- **Accepts connections from other machines** by default (binds `0.0.0.0:6379`).

The RESP parser, the encoder and a small client (`micro-redis-cli`) are written from scratch in this repository. No Redis or RESP crates are used. Lua comes from the `mlua` crate (Lua 5.1, which is what Redis embeds).

---

## Contents

1. [Quick start](#quick-start)
2. [Talking to the server](#talking-to-the-server)
3. [Running the tests](#running-the-tests)
4. [How each requirement is met](#how-each-requirement-is-met)
5. [Extra credit: running the distributed crawler](#extra-credit-running-the-distributed-crawler)
6. [How the service stays linearizable](#how-the-service-stays-linearizable)
7. [Performance and design choices](#performance-and-design-choices)
8. [How it was verified](#how-it-was-verified)
9. [Project layout](#project-layout)
10. [Spec decisions and differences from real Redis](#spec-decisions-and-differences-from-real-redis)
11. [Checking every requirement](#checking-every-requirement)
12. [Demo script](#demo-script)

---

## Quick start

**Requirements:** Rust 1.88 or newer, because the code uses edition 2024. Install it with [rustup](https://rustup.rs). `redis-cli` is optional; the repo ships its own CLI.

```bash
# 1. Build
cargo build --release

# 2. Start the server (default: port 6379, all network interfaces)
cargo run --release -- --port 6379

# 3. In another terminal
redis-cli -p 6379 PING          # → PONG
```

### Connecting from another machine

The server binds `0.0.0.0` by default, so any machine that can reach yours on the network can connect.

```bash
# On the server machine: find its address
ipconfig getifaddr en0          # macOS (Wi-Fi)
hostname -I                     # Linux

# On the other machine (say the server is 192.168.1.20)
redis-cli -h 192.168.1.20 -p 6379 PING            # → PONG
redis-cli -h 192.168.1.20 -p 6379 -n 3 RPUSH q a  # database 3
```

If it doesn't connect: both machines must be on the same network, and the server's firewall must allow the port. On macOS, accept the "allow incoming connections" prompt the first time `micro-redis` runs (or add it under System Settings → Network → Firewall). On Ubuntu with ufw: `sudo ufw allow 6379/tcp`. Campus and café Wi-Fi often block traffic between devices; a phone hotspot works for demos.

To accept only this machine, start with `--bind 127.0.0.1`. The server has no password, so don't expose it to the internet.

Server options:

| Flag | Meaning | Default |
| --- | --- | --- |
| `-p`, `--port <PORT>` | TCP port to listen on | `6379` |
| `-b`, `--bind <ADDR>` | Address to bind. `0.0.0.0` accepts other machines; `127.0.0.1` accepts only this one | `0.0.0.0` |
| `-h`, `--help` | Print help | |

Logging uses the `RUST_LOG` environment variable. `RUST_LOG=debug cargo run --release` prints every connection and command, which is useful on demo day.

If you don't have `redis-cli`, install it with `brew install redis` on macOS or `sudo apt install redis-tools` on Debian/Ubuntu. Or use the bundled client, described next.

---

## Talking to the server

Clients checked against this server: `redis-cli` 7.0, `redis-benchmark`, the Rust `redis` crate 1.7 (including `MultiplexedConnection` and `redis://host:port/3` URLs), and redis-py 8.1. redis-py 8 asks for RESP3 by default (`HELLO 3`), and the spec only requires RESP2, so create it with `redis.Redis(port=6379, db=3, protocol=2)`.

### With `redis-cli`

```bash
redis-cli -p 6379 SET greeting hello
redis-cli -p 6379 GET greeting                # "hello"
redis-cli -p 6379 -n 3 GET greeting           # (nil): database 3 is separate
redis-cli -p 6379 RPUSH jobs a b c            # (integer) 3
redis-cli -p 6379 BLPOP jobs 0                # 1) "jobs" 2) "a"
redis-cli -p 6379 BRPOP empty 1.5             # (nil) after 1.5 s
```

**Blocking example (two terminals):**

```bash
# terminal 1: waits
redis-cli -p 6379 BLPOP queue 0
# terminal 2: releases it
redis-cli -p 6379 RPUSH queue hello
```

**Selecting a database:** `SELECT` is optional. A client that never sends it uses **database 0**. To use another database, pass `-n <db>` to `redis-cli`: it sends `SELECT` as the very first command, which is the only place the course spec allows it. In interactive `redis-cli` without `-n`, typing `SELECT 3` later is rejected on purpose. See [Spec decisions](#spec-decisions-and-differences-from-real-redis).

### With the bundled client, `micro-redis-cli`

This client is built on this crate's own RESP parser.

```bash
cargo run --release --bin micro-redis-cli -- -p 6379 SET k "hello world"
cargo run --release --bin micro-redis-cli -- -p 6379 -n 3 GET k
cargo run --release --bin micro-redis-cli -- -p 6379        # interactive prompt
```

After `cargo build --release`, both binaries are in `target/release/`: `micro-redis` and `micro-redis-cli`.

### With `nc` / `telnet`

The server also accepts inline commands, like real Redis does:

```bash
nc localhost 6379
PING            # ← type this, the server answers +PONG
SET a 1         # +OK
GET a           # $1 / 1
```

### From Rust

```rust
use micro_redis::client::Client;

let mut c = Client::connect("127.0.0.1:6379").await?;
c.rpush("q", &["job-1"]).await?;
let popped = c.blpop(&["q"], 0.0).await?;   // Some(("q", "job-1"))
```

---

## Running the tests

```bash
cargo test            # everything: unit + integration
cargo test --release  # the same suite, faster (about 15 s)
```

100 tests in total. `tests/compat.rs` needs a real `redis-server` on the `PATH` (`sudo apt install redis-server` or `brew install redis`). Without one it prints `SKIPPED` and passes.

| Suite | File | What it checks |
| --- | --- | --- |
| Unit: RESP | `src/resp/` | Every RESP2 type round-trips. Every prefix of a frame counts as "incomplete". Garbage, huge lengths and deep nesting are rejected. 20,000 random byte strings never panic the parser. |
| Unit: commands, session, config | `src/command/`, `src/server/session.rs`, `src/config.rs` | Arity, case-insensitivity, SELECT and timeout validation, the 5-key limit, exact error text. |
| Unit: database | `src/db/` | Push order, empty-list deletion, FIFO hand-off inside the push, variadic pushes, multi-key waiters, cancel-vs-serve, dead receivers, invariants after every step. |
| Protocol | `tests/protocol.rs` | Exact bytes on the wire: byte-by-byte input, binary values, every error reply, inline commands, pipelining, protocol errors closing the connection. |
| Commands | `tests/commands.rs` | Each command over TCP, the 16 databases, the SELECT rule, 300 simultaneous connections, both binaries and the `--port` flag, and a connection through the machine's network address (with `--bind 127.0.0.1` refusing it). |
| Concurrency | `tests/concurrency.rs` | FIFO serving, no lost wake-ups, newcomers never overtaking waiters, timeout-vs-push and disconnect-vs-push races, and a conservation stress test. |
| Linearizability | `tests/linearizability.rs` | 250 random concurrent histories, plus 150 bigger ones spread over three databases at once, are run through a linearizability checker. The checker itself is also tested. |
| Extended commands | `tests/extended.rs` | Database 0 without SELECT, `MULTI`/`EXEC`/`DISCARD`/`EXECABORT`, transactions never seen half-applied, `EVAL`/`EVALSHA`/`SCRIPT`, scripts losing no updates under 16 concurrent clients, a crawler-style "claim a URL" script never handing a URL out twice, expiry (lazy and background), `FLUSHALL`. |
| Unit: scripting, glob | `src/scripting/`, `src/db/glob.rs` | Lua ↔ RESP conversions, sandbox, error format, SHA1 values, `KEYS` pattern matching. |
| Differential | `tests/compat.rs` | Starts a real `redis-server` and sends both servers the same ~230 edge cases plus 5,000 random commands, blocking pops included. Every reply must be byte-identical. |

Heavy tests read `STRESS_ITERS` to repeat more, for example `STRESS_ITERS=2000 cargo test --release --test concurrency no_lost_wakeups`. The differential test also reads `COMPAT_SEED`: `COMPAT_SEED=7 STRESS_ITERS=50000 cargo test --release --test compat`. To see the statistics the tests print, add `-- --nocapture`.

---

## How each requirement is met

| Requirement (Mini 2 spec) | How | Where |
| --- | --- | --- |
| Redis-compatible service in Rust on tokio | tokio multi-thread runtime; `redis-cli` and `redis-benchmark` work against it | `src/main.rs`, `src/server/` |
| Single machine; only our own code can fail | All data lives in memory. A panic in a critical section would leave a database half-updated, so the build sets `panic = "abort"`: the service stops instead of serving inconsistent data | `Cargo.toml` |
| Multithreaded and asynchronous | One tokio task per connection, scheduled across worker threads. Blocked clients hold no thread and no lock | `src/server/connection.rs`, `src/server/blocking.rs` |
| Linearizable | One mutex per database, one critical section per command, reply only after it ends, and blocked clients served *inside* the push's critical section | `src/db/`, [below](#how-the-service-stays-linearizable) |
| 16 independent databases, default 0 | `Server` holds 16 `Db`s; a connection starts on db 0, so `SELECT` is optional | `src/server/mod.rs`, `src/server/session.rs` |
| `SELECT` only at the start of a session | Accepted only before any other command has been executed on the connection. Later attempts get `-ERR SELECT is only allowed as the first command` | `src/server/session.rs` |
| `GET`, `EXISTS`, `PING` as in Redis | Same replies and errors as Redis 7. They were cross-checked against a real `redis-server` | `src/db/state.rs`, `src/command/`, `src/error.rs` |
| `SET` without options | `SET key value` behaves exactly as Redis. The crawler needs Redis's options too (`NX`, `XX`, `GET`, `EX`, `PX`, `EXAT`, `PXAT`, `KEEPTTL`), so they are supported as a superset | `src/db/ops/strings.rs` |
| `LPUSH`, `RPUSH` as in Redis | Variadic; returns the length after the push; creates the list; `WRONGTYPE` on strings | `src/db/state.rs` |
| `BLPOP`, `BRPOP` as in Redis, at most 5 keys, timeout a double | Keys scanned left to right; oldest waiter served first; timeout is `f64` seconds and `0` means forever; more than 5 keys gives `-ERR BLPOP accepts at most 5 keys` | `src/db/`, `src/command/` |
| `PING` exempt from linearizability | Answered without touching any database | `src/server/dispatch.rs` |
| RESP2 | Own parser and encoder for all five RESP2 types, plus inline commands | `src/resp/` |
| Configurable port, default 6379 | `--port` / `-p`. Binds `0.0.0.0` by default so other machines can connect (`--bind` to change) | `src/config.rs` |
| No pipelining needed | Not needed, but supported and fast: requests are answered strictly in order, consecutive data commands run as one atomic step, and replies go out in one write (about 3× faster pipelines, see [Batched replies](#batched-replies-and-batched-pipelines)) | `src/server/connection.rs` |
| No persistence needed | Nothing touches disk | — |
| Extensive programmatic tests, `cargo test` | 100 tests, see [Running the tests](#running-the-tests) | `tests/`, `src/*` |
| README on linearizability and how to run | This file | |
| Optional: linearizability checker | Done, as a hand-written Wing & Gong checker | `tests/linearizability.rs` |
| Extra credit: run the Mini 1 distributed crawler | All commands and Lua scripts it uses; its e2e suite passes | [below](#extra-credit-running-the-distributed-crawler) |

---

## Extra credit: running the distributed crawler

Point the crawler at micro-redis instead of Redis. Nothing in the crawler changes:

```bash
cargo run --release -- --port 6379          # micro-redis in place of redis-server
REDIS_URL=redis://127.0.0.1:6379/14 crawl node   # or without /14: database 0, no SELECT sent
```

**What the crawler sends** was recorded with `MONITOR` on real Redis during its full test run:

- Top level: `DEL EVAL EVALSHA EXEC EXISTS FLUSHDB GET HDEL HGET HGETALL HSET INCRBY LLEN MULTI SCRIPT LOAD SELECT SETEX SMEMBERS`.
- Inside its Lua scripts: `DECR HGET HINCRBY HSET INCRBY KEYS LINDEX LLEN LPOP LREM RPUSH SADD SET SISMEMBER SREM`.
- `redis-rs` scripts first try `EVALSHA` and fall back to `SCRIPT LOAD` when they get `NOSCRIPT`; micro-redis returns that exact error.
- `redis-rs` atomic pipelines arrive as `MULTI … EXEC`.

**Result.** The crawler's `scripts/e2e_test.sh` was run against real Redis 7 and against micro-redis. All 7 checks pass on both, with identical statistics: 1 node vs 3 nodes, two jobs at once, resubmission, the extra link rules, a 221-file slow site, a node killed with `kill -9` while holding URLs, and a frozen "zombie" node whose late reports must be rejected.

**Commands supported** (each behaves as in Redis 7; the differential test checks replies and error texts against a real `redis-server`):

| Group | Commands |
| --- | --- |
| Connection | `PING` `ECHO` `SELECT` `QUIT` `CLIENT SETNAME/GETNAME/SETINFO/ID` `COMMAND` |
| Keys | `DEL` `UNLINK` `EXISTS` `TYPE` `KEYS` `DBSIZE` `FLUSHDB` `FLUSHALL` `EXPIRE` `PEXPIRE` `TTL` `PTTL` `PERSIST` |
| Strings | `GET` `SET` (all options) `SETEX` `PSETEX` `SETNX` `GETSET` `GETDEL` `MGET` `MSET` `INCR` `DECR` `INCRBY` `DECRBY` `APPEND` `STRLEN` |
| Lists | `LPUSH` `RPUSH` `LPUSHX` `RPUSHX` `LPOP` `RPOP` (with count) `LLEN` `LRANGE` `LINDEX` `LSET` `LREM` `LTRIM` `RPOPLPUSH` `LMOVE` `BLPOP` `BRPOP` |
| Sets | `SADD` `SREM` `SISMEMBER` `SMISMEMBER` `SCARD` `SMEMBERS` |
| Hashes | `HSET` `HMSET` `HSETNX` `HGET` `HMGET` `HGETALL` `HDEL` `HLEN` `HEXISTS` `HINCRBY` `HKEYS` `HVALS` |
| Transactions | `MULTI` `EXEC` `DISCARD` |
| Scripting | `EVAL` `EVALSHA` `SCRIPT LOAD/EXISTS/FLUSH`, with `redis.call`, `redis.pcall`, `redis.error_reply`, `redis.status_reply`, `redis.sha1hex`, `redis.log` |

**Expiry** works like Redis: a key is checked when it is touched (lazy expiry), and a background task removes up to 200 expired keys per database every 100 ms (active expiry). Inside one command, script or transaction the clock is frozen, so a key cannot expire halfway through.

**Lua scripting.** Each `EVAL` runs Lua 5.1 in a fresh sandboxed interpreter:

- Undeclared globals cannot be read or written.
- `loadstring`, `dofile`, `print` and the `os`/`io` libraries are not available.
- Values are converted between Lua and RESP with Redis's rules: `false` becomes nil, `{ok=…}` a status reply, `{err=…}` an error reply, and numbers are truncated to integers.
- Errors carry Redis's suffix, for example `ERR value is not an integer or out of range script: <sha>, on @user_script:1.`.
- Blocking commands are refused inside scripts, as in Redis.

The script is compiled *before* the database lock is taken, so a syntax error or a slow compile never holds the lock.

---

## How the service stays linearizable

### What we must guarantee

Every operation must appear to take effect at a single instant, its **linearization point (LP)**. That instant must lie between when the server receives the request and when it sends the reply. The replies everyone sees must also match running all operations one at a time in LP order. In particular, if command A finished before command B started, B must see A's effect.

### The model

```text
Server ── db[0] ── Mutex<State>   State = { data:    key → String | List (never empty),
       ├─ db[1] ── Mutex<State>             waiters: id  → (keys, end, oneshot sender),
       ...                                   queues:  key → waiter ids, oldest first }
       └─ db[15]
```

- **One mutex per database.** Every command touches exactly one database, so one lock is enough to make each command atomic. Different databases never contend.
- **One critical section per command.** Each command does all of its reads and writes inside one `lock()…unlock()`. All validation (arity, timeout parsing, the 5-key rule) happens *before* locking, so critical sections do only data work and never `.await`.
- **Reply after unlock.** The connection writes the reply only after the critical section has ended, so a client can never learn about an effect before it is visible to everyone. (`bug-reply-before-apply` plants exactly this mistake, and the tests catch it. See [verification](#how-it-was-verified).)

With these three rules, the LP of every non-blocking command is "some instant while it holds its database lock". Each LP is inside the request/response interval, and no other command can observe a half-done state.

### Linearization points

| Command | Linearization point |
| --- | --- |
| `GET`, `SET`, `EXISTS`, `LPUSH`, `RPUSH` | Inside its single critical section on the selected database |
| `BLPOP`/`BRPOP` that finds data | Inside its critical section, at the pop |
| `BLPOP`/`BRPOP` that blocks and is **served** | Immediately after the LP of the push that served it, inside that push's critical section |
| `BLPOP`/`BRPOP` that **times out** | Its registration critical section. At that instant all its keys were empty or absent, which is exactly the state in which `nil` is the correct answer |
| Other single-key and multi-key data commands (`INCR`, `HSET`, `SADD`, `KEYS`, ...) | Inside its single critical section |
| `MULTI … EXEC` | `EXEC`'s critical section: every queued command runs in one lock hold, then waiters are served |
| `EVAL` / `EVALSHA` | The script's critical section: every `redis.call` runs inside one lock hold |
| `FLUSHALL` | The instant all 16 locks are held (taken in index order, so no deadlock) |
| Reading an expiring key | Inside the critical section; expiry is decided against the section's frozen clock |
| `SELECT` | Touches no shared data; it only changes the connection's own state |
| `PING` | Exempt (spec) |

### Blocking commands, step by step

**1. Check and register in one critical section** (`Db::pop_or_register`). Under the lock, BLPOP scans its keys left to right and pops from the first non-empty list. If all keys are empty, it registers as a waiter *before releasing the lock*. It gets an id and is appended to the FIFO queue of each of its keys.

If checking and registering were two critical sections, a push could slip into the gap. The client would then sleep on a non-empty list forever: a *lost wake-up*. `bug-split-register` plants this, and `no_lost_wakeups` catches it.

**2. Wait without holding anything.** The connection task awaits a `tokio::sync::oneshot` receiver, holding no lock and no thread. Inside a `tokio::select!` it also watches the timeout and the socket, so it notices a disconnect.

**3. A push serves waiters inside its own critical section** (`State::serve`). After LPUSH/RPUSH has added *all* its values, and while it still holds the lock, it does the following:

- takes the **oldest** waiter queued on that key;
- removes it from the registry and from the queues of all its keys, so a waiter on 5 keys can be served only once;
- pops one element from the waiter's end (left for BLPOP, right for BRPOP);
- sends `(key, value)` through the waiter's oneshot channel (`send` never blocks);
- repeats while the list is non-empty and someone is waiting.

Only then is the lock released. Between the push and the hand-off, no other command can run on that database. That is why the served BLPOP's LP can sit immediately after the push's LP, with nothing in between.

The alternative, "notify the waiter and let it re-acquire the lock to pop", leaves a gap where a newcomer can grab the element ahead of a client that has waited longer. That breaks Redis's FIFO guarantee. `bug-split-serve` plants this, and `waiting_client_is_never_overtaken_by_a_newcomer` catches it.

This matches Redis's documented semantics:

- A variadic push completes before anyone is served. With `BLPOP foo 0` waiting, `LPUSH foo a b c` delivers `c`.
- The push's integer reply is the length before serving.
- A served client gets the element from the first non-empty key *in its own key order*. If one of its earlier keys has meanwhile become a string through `SET`, it gets `WRONGTYPE`, exactly as a fresh BLPOP would.

**4. Every way a wait ends is decided exactly once, under the lock.**

| Event | What the task does | Why it is safe |
| --- | --- | --- |
| Value arrives | Reply `[key, value]` | The pop already happened inside the push |
| Timeout fires | Lock, then try to remove its own registration (`Db::cancel`). **Removed:** never served, so reply `nil`. **Not found:** a push won the race a moment earlier, so the value is already in the receiver; reply with it | Serve and cancel both run under the same lock, so exactly one wins. Values are never lost or duplicated |
| Client disconnects | Same `cancel`. If a push had already served it, the value is dropped and counted in `Server::lost_deliveries`, with a warning log | Same as real Redis: an element popped for a client that vanished is gone. Putting it back later would let other clients see the list empty and then non-empty again with no push in between, which is not linearizable |
| Receiver gone without cancel (task died) | `send` fails, so `serve` puts the element back at the same end and serves the next waiter | Nothing observable happened |

### Transactions and scripts are one critical section too

`Db::critical(f)` is the single way into a database. It takes the lock, freezes the clock, runs `f`, serves any blocked clients whose keys received data, and only then unlocks. A single command, a whole `EXEC`, and a whole Lua script are all just different `f`s. So the earlier argument carries over unchanged: everything a transaction or script does happens at one instant, other clients see all of it or none of it, and the reply is sent after the unlock.

Two details match Redis:

- **Waiters are served at the end, not after each push.** A script that does `RPUSH q x` and then `LPOP q` sees its own `x`. A client blocked on `q` is served only after the script finishes, from whatever is left.
- **No rollback.** If a command inside `EXEC` or a script fails, the earlier ones stay applied. That is Redis's documented behaviour. It is still atomic: nobody saw the intermediate state.

`tests/extended.rs` checks both properties under load. 16 clients run a Lua read-modify-write 200 times each and no increment is lost. Transactions moving a unit between two counters are never observed half-applied.

### Invariants (checked in tests by `Server::check_invariants`)

1. No key holds an empty list, set or hash. Emptied ones are deleted, so `EXISTS` behaves as in Redis. Every TTL belongs to an existing key.
2. Every queued id belongs to a live waiter, and every waiter appears exactly once in the queue of each of its keys.
3. No waiter is queued on a key that holds a non-empty list. If one were, a push would have failed to serve it. This invariant is what makes the timed-out BLPOP's `nil` correct.

### Why one lock per database and not finer locking?

`EXISTS`, `BLPOP`, `RPOPLPUSH`, transactions and scripts span several keys. Per-key locks would need ordered multi-lock acquisition and careful wake-up logic across locks. That adds risk without any benefit at this scale, and different databases already run in parallel. The other alternative considered, one actor task per database fed by a channel, is described under [Performance and design choices](#performance-and-design-choices).

---

## Performance and design choices

### Why a mutex per database

Each database is one `Mutex<State>`. A connection's task takes the lock, runs the command, serves any blocked clients, releases the lock, and only then writes the reply. Nothing is awaited while the lock is held, so a lock hold lasts well under a microsecond and a plain `std::sync::Mutex` is the right tool.

**Alternative tried and dropped: one actor task per database.** Each database's data was owned by one tokio task, and connections sent it jobs over an `mpsc` channel and awaited a `oneshot` reply. It was equally linearizable and passed the whole test suite, but every command paid for a hand-off: wake the database task, then wake the connection task again with the reply. A lock with no other holder costs one atomic instruction instead. Measured on a 2-core VM and an 8-core Mac:

- With one command at a time, which is the workload the spec describes, the two were tied or the lock was ahead.
- With pipelined clients the lock was faster in every round on the VM (1.57×). On the Mac, with traffic spread over all 16 databases, the two stayed within 6% of each other at every thread count from 1 to 16, with the lock ahead at four of the five.

The actor added code (a barrier for `FLUSHALL`, Lua compiled on the database task) without being faster, so the project keeps the lock only.

### Batched replies and batched pipelines

The spec doesn't require pipelining, but the crawler (extra credit) uses it. A pipelining client sends many commands before reading any reply, and the connection loop does two things for it:

- **Replies are batched.** Replies collect in the output buffer and go out in one `write()` once no complete command is left to read: before reading more bytes, before a BLPOP starts waiting, and before closing.
- **Commands are batched.** Consecutive plain data commands already in the buffer (up to 512) run in **one** lock hold. A rejected command, `SELECT`, `MULTI`/`EXEC`, `EVAL` or a blocking pop ends the batch and runs on its own, in order.

This is still linearizable. Every command in a batch had already been received and none had been answered, so running them in order inside one critical section puts each command's instant inside its own request/reply interval. `pipelined_mixed_commands_answer_in_order` checks the replies of a 1,000-command pipeline byte for byte.

**Before and after.** Medians of 100 interleaved rounds per server, in requests per second (`redis-benchmark`, 50 clients, 2-core VM shared with the benchmark tool). The range is the middle 50% of rounds.

| Workload | Before batching | After batching | Real Redis 7 |
| --- | --- | --- | --- |
| SET, one at a time | 104.9k (97k–111k) | 102.5k (97k–108k) | 100.0k (93k–105k) |
| SET, pipelined 16 deep | 197k (186k–205k) | **771k (716k–821k)** | 1,294k (1,188k–1,397k) |

- **Pipelined** got 3.9× faster (every one of 100 rounds), reaching 0.60× of Redis.
- **One at a time** didn't change and ties with Redis: the network system calls (about 10 µs per request) dominate, and there is nothing to batch.
- **Lua is the remaining big gap:** about 150 µs per `EVAL` against Redis's 12 µs, almost all of it building a fresh Lua interpreter for each script.

### Benchmarking on your own machine

```bash
bash scripts/bench.sh            # all traffic on database 0
bash scripts/bench_multidb.sh    # traffic spread over all 16 databases
ROUNDS=20 THREADS="1 4 8" bash scripts/bench.sh
```

Both start micro-redis and a real `redis-server`, run each workload `ROUNDS` times for each tokio worker-thread count in `THREADS` (passed to the server as `TOKIO_WORKER_THREADS`), and print medians with the middle 50% of rounds. Run them with `bash`, not `sudo`. Compare micro-redis with Redis **within the same block of rows**: a laptop speeds up and slows down over a long run (power management, background work), so numbers from different blocks aren't comparable.

---

## How it was verified

1. **Property tests for concurrency** (`tests/concurrency.rs`):
   - FIFO serving.
   - No lost wake-ups: 300 BLPOP/RPUSH races.
   - Newcomers never overtake a waiting client.
   - Timeout-vs-push and disconnect-vs-push races, each checked to put every value in exactly one place.
   - A conservation stress test: 6 pushers, 6 poppers mixing BLPOP and BRPOP on random key subsets, random timeouts and clients hanging up mid-wait. At the end, `received + still in lists + lost to disconnects == pushed`, with zero duplicates.
2. **Linearizability checker** (`tests/linearizability.rs`). 4 clients × 5 random operations are recorded with invoke and complete times. A memoized Wing & Gong search then looks for a legal sequential order against a simple model of the spec. 250 histories are checked per run.
   - The recorded intervals are wider than the server's real ones, so the checker cannot report false violations.
   - A unit test makes sure the checker *rejects* stale reads, duplicate deliveries and missed values. A checker that accepts everything would prove nothing.
3. **Planted bugs.** The crate has three cargo features that each break one rule on purpose. Every one of them makes the suite fail:

   | Feature | Mistake it plants | Caught by |
   | --- | --- | --- |
   | `bug-reply-before-apply` | `SET` replies `OK` before applying | `read_after_acknowledged_write_from_another_client` and the random-history checker (not linearizable) |
   | `bug-split-register` | BLPOP checks keys and registers in two critical sections | `no_lost_wakeups` (lost wake-up) |
   | `bug-split-serve` | Push releases the lock before serving waiters | `waiting_client_is_never_overtaken_by_a_newcomer` (FIFO broken) |

   ```bash
   cargo test --release --no-fail-fast --features bug-split-register   # expect failures
   ```

4. **Differential testing against real Redis** (`tests/compat.rs`). Both servers get the same commands in lockstep, and every reply must be identical, including error texts. About 230 hand-written edge cases are followed by random commands over a few shared keys, so types collide and keys expire.
   - Default run: 5,235 commands, all identical.
   - Before release it was run with 50,000 commands on 4 seeds, all identical. That longer run found two real bugs, both fixed and kept as regression cases:
     - `LINDEX`/`LSET` must look up the key before parsing the index.
     - `RPOPLPUSH k k` on a one-element list must not drop the key's TTL.
5. **The crawler's own end-to-end suite** passes against micro-redis. See [Extra credit](#extra-credit-running-the-distributed-crawler).
6. **Hygiene.**
   - No `unwrap()` outside test code.
   - `cargo clippy --all-targets -- -D warnings` is clean.
   - The stress suites passed 10 consecutive runs.
7. **Sanity benchmark.** These numbers are not a requirement. `redis-benchmark -p 7001 -t set,get,lpush -c 50 -n 100000 -q` on a 2-vCPU Xeon @ 2.8 GHz cloud VM, release build, gave roughly 108k SET/s, 92k GET/s and 108k LPUSH/s, with p50 ≈ 0.25 ms.

---

## Project layout

```text
micro-redis/
├── Cargo.toml                   # deps: tokio, bytes, tracing, thiserror, mlua, sha1_smol; bug-* features; panic=abort
├── README.md
├── .github/workflows/ci.yml     # fmt + clippy -D warnings + tests
├── scripts/bench.sh             # benchmark against redis-server, database 0
├── scripts/bench_multidb.sh     # the same, spread over all 16 databases
├── src/
│   ├── main.rs                  # server binary: parse flags, start logging, run
│   ├── lib.rs                   # module map, run(addr)
│   ├── config.rs                # --port / --bind parsing and help text
│   ├── error.rs                 # every error reply text, in one place
│   ├── resp/                    # RESP2, written from scratch
│   │   ├── frame.rs             #   the Frame type, redis-cli style display
│   │   ├── parse.rs             #   incremental parser, safety limits
│   │   ├── inline.rs            #   inline commands ("PING\r\n" from nc), argument splitting
│   │   └── encode.rs            #   frames to bytes
│   ├── command/                 # request → typed, validated Command
│   │   ├── mod.rs               #   Command enum: connection, MULTI, EVAL, SCRIPT, CLIENT, data commands
│   │   └── args.rs              #   argument parsing: db index, timeout, numkeys
│   ├── db/                      # one database
│   │   ├── mod.rs               #   Db: critical(), the only place the lock is taken
│   │   ├── state.rs             #   keyspace, expiry, typed access, serve(), invariants
│   │   ├── ops/                 #   every data command: table (name, arity) + executors
│   │   │   ├── mod.rs           #     Op table, DataCmd, dispatch
│   │   │   ├── keys.rs          #     DEL, KEYS, EXPIRE, TTL, ...
│   │   │   ├── strings.rs       #     GET, SET + options, INCR family, ...
│   │   │   ├── lists.rs         #     pushes, pops, LRANGE, LREM, LMOVE, ...
│   │   │   ├── sets.rs
│   │   │   └── hashes.rs
│   │   ├── glob.rs              #   KEYS pattern matching
│   │   ├── waiters.rs           #   registry of blocked clients, FIFO per key
│   │   └── value.rs             #   String | List | Set | Hash
│   ├── scripting/               # EVAL
│   │   ├── mod.rs               #   script cache (SHA1), compile outside the lock, run inside it
│   │   └── lua.rs               #   sandbox, redis.call/pcall, Lua ↔ RESP conversion
│   ├── server/                  # networking
│   │   ├── mod.rs               #   Server (16 dbs) and the accept loop
│   │   ├── connection.rs        #   per-client loop: read, parse, dispatch, reply
│   │   ├── dispatch.rs          #   run one command against the selected db
│   │   ├── blocking.rs          #   waiting for a blocked BLPOP/BRPOP to end
│   │   ├── session.rs           #   selected db, the SELECT rule, MULTI state
│   │   ├── transaction.rs       #   queued commands and EXEC
│   │   └── reply.rs             #   building reply frames
│   ├── client.rs                # async RESP2 client (own parser)
│   └── bin/micro-redis-cli.rs   # redis-cli-like CLI on top of client.rs
└── tests/
    ├── common/mod.rs            # start a server on a free port, tiny PRNG
    ├── protocol.rs
    ├── commands.rs
    ├── concurrency.rs
    ├── linearizability.rs
    ├── extended.rs              # crawler commands: MULTI, EVAL, expiry, ...
    └── compat.rs                # differential test against a real redis-server
```

Unit tests sit next to the code they test: at the bottom of each file, or in a `tests.rs` beside `mod.rs` (`src/db/tests.rs`, `src/command/tests.rs`).

**Dependency direction.** `resp` uses nothing else in the crate, and `db` uses only `resp` and `error`. `scripting` runs on top of `db`. `command` uses `resp` (and borrows `db::End`), `error` builds replies from both, and `server` ties everything together. So the parser and the database logic can each be read and tested on their own.

Dependencies are deliberately few: `tokio`, `bytes`, `tracing`, `tracing-subscriber`, `thiserror`, `mlua` (Lua 5.1, built from source by the crate, so no system Lua is needed) and `sha1_smol` (script SHAs). Tests use no extra crates.

---

## Spec decisions and differences from real Redis

Where the course spec and Redis disagree, the spec wins:

- **`SELECT` is optional, and only at the start of a session.** A client that never sends `SELECT` works on database 0. If it does send one, `SELECT` must be the first command the server *executes* on the connection. Any executed command before it, even `PING`, ends the window, and so does a successful `SELECT`. A late `SELECT` replies `-ERR SELECT is only allowed as the first command` and does not change the database.
  - *Rejected* requests (unknown commands, wrong arguments) have no effect, so they don't end the window.
  - Connection-setup commands also leave the window open: `CLIENT SETNAME/SETINFO/GETNAME/ID` and `COMMAND`. Client libraries send these while connecting. redis-py sends `CLIENT SETINFO …` before `SELECT`; if that counted, `redis.Redis(db=3)` could never select database 3.
  - `redis-cli -n <db>` and `redis-rs` with a `/db` URL send `SELECT` first, so they work.
- **`SET` options are supported** even though the spec only requires bare `SET`, because the crawler uses `SET … NX`/`EX` patterns. Bare `SET k v` is unchanged.
- **Not supported:**
  - `SELECT` inside a Lua script (Redis allows it; here it returns an error, since a script runs inside one database's critical section).
  - `WATCH`/`UNWATCH`.
  - Commands outside the table above, for example sorted sets, `SCAN`, pub/sub and `INFO`.
  - `COMMAND` replies with an empty list, so interactive `redis-cli` shows no hints.
- **No script timeout.** A script with an infinite loop holds its database forever, and `SCRIPT KILL` is not implemented. Redis has the same risk, softened by `SCRIPT KILL`.
- **At most 5 keys per `BLPOP`/`BRPOP`.** More keys reply `-ERR BLPOP accepts at most 5 keys` (or `BRPOP`).
- **Timeouts.**
  - The timeout is a double in seconds, and `0` means forever, as in Redis 6+.
  - A negative timeout is an error, and so is a non-number. `NaN` counts as a non-number.
  - `inf`, or anything above 10 years, is treated as forever.
- **Protocol errors** (malformed RESP) get `-ERR Protocol error: …` and the connection is closed, because the stream can no longer be framed. Every other error leaves the connection usable.
- **Limits:** bulk strings up to 512 MB (Redis's default), arrays up to 1,048,576 elements, header lines up to 64 KB, and nesting up to 32 levels. An announced length is never allocated before the bytes arrive.

---

## Checking every requirement

Why these tests are enough to show each requirement is met, including the linearizability argument, is written up in [TESTING.md](TESTING.md).

### 1. The automated suite (do this first)

```bash
cargo fmt --check
cargo clippy --all-targets -- -D warnings
cargo test --release                       # 100 tests, about 15 s; all must pass
```

Then check that the tests can actually catch mistakes. Each line below plants one bug and **must fail**. `--no-fail-fast` keeps going after the first failing file, so the summary at the end lists every test that caught it:

```bash
cargo test --release --no-fail-fast --features bug-reply-before-apply   # reply before the write is applied
cargo test --release --no-fail-fast --features bug-split-register       # BLPOP lost wake-up
cargo test --release --no-fail-fast --features bug-split-serve          # newcomer overtakes a waiting client
```

Expected catches: `read_after_acknowledged_write_from_another_client` and `random_histories_are_linearizable` for the first, `no_lost_wakeups` for the second, `waiting_client_is_never_overtaken_by_a_newcomer` for the third. Other tests may fail as well (a broken server breaks many things); what matters is that these do.

For extra confidence, repeat the concurrency tests many times. The tests open hundreds of thousands of short connections; they close them with a reset, so they don't use up the operating system's local ports, and they retry any connection the OS turns away (macOS only queues 128 waiting connections):

```bash
STRESS_ITERS=2000 cargo test --release --test concurrency
STRESS_ITERS=2000 cargo test --release --test linearizability
COMPAT_SEED=7 STRESS_ITERS=50000 cargo test --release --test compat   # needs redis-server
```

### 2. Requirement by requirement

Start the server in one terminal with `cargo run --release` (port 6379), then use `redis-cli` in another. "Test" names the automated test that covers the same thing; run one with `cargo test --release <name>`.

| Requirement | Test | Check by hand | Expected |
| --- | --- | --- | --- |
| Port 6379 by default, configurable | `server_binary_honours_port_flag_and_cli_binary_talks_to_it` | `cargo run --release -- --port 7000`, then `redis-cli -p 7000 PING` | `PONG` |
| RESP2 | `every_reply_type_is_exact`, `replies_match_real_redis` | `printf '*1\r\n$4\r\nPING\r\n' \| nc localhost 6379` | `+PONG` |
| `PING` | `ping_with_message` | `redis-cli PING hi` | `"hi"` |
| `SET` / `GET` | `set_get_overwrite` | `redis-cli SET k v`, then `redis-cli GET k` | `OK`, then `"v"` |
| `EXISTS` | `exists_counts_repeats` | `redis-cli EXISTS k k nope` | `(integer) 2` |
| `LPUSH` / `RPUSH` | `push_returns_length_and_lpush_reverses` | `redis-cli LPUSH l a b c`, then `redis-cli LRANGE l 0 -1` | `3`, then `c b a` |
| 16 independent databases | `select_isolates_databases` | `redis-cli -n 5 SET x five`, then `redis-cli GET x`, then `redis-cli -n 5 GET x` | `(nil)`, then `"five"` |
| Database 0 without `SELECT` | `database_0_is_the_default_without_select` | `redis-cli SET d0 yes`, then `redis-cli -n 0 GET d0` | `"yes"` |
| `SELECT` only first | `select_only_as_first_command` | `redis-cli`, type `GET x`, then `SELECT 3` | `ERR SELECT is only allowed as the first command` |
| `BLPOP` returns at once if data exists | `blpop_takes_first_non_empty_key_without_blocking` | `redis-cli RPUSH q1 a`, then `redis-cli BLPOP q0 q1 0` | `q1`, `a` immediately |
| `BLPOP` / `BRPOP` block until a push | `waiters_are_served_first_come_first_served` | Terminal A: `redis-cli BLPOP jobs 0`. Terminal B: `redis-cli RPUSH jobs x` | A prints `jobs`, `x` |
| `BRPOP` pops from the tail | `brpop_pops_from_the_tail` | `redis-cli RPUSH t a b`, then `redis-cli BRPOP t 0` | `t`, `b` |
| Timeout is a double | `blpop_times_out_with_nil_after_the_timeout` | `time redis-cli BLPOP none 1.5` | `(nil)` after about 1.5 s |
| At most 5 keys | `errors_keep_the_connection_usable` | `redis-cli BLPOP a b c d e f 1` | `ERR BLPOP accepts at most 5 keys` |
| Oldest waiter served first | `waiters_are_served_first_come_first_served`, `waiting_client_is_never_overtaken_by_a_newcomer` | A: `BLPOP f 0`, then B: `BLPOP f 0`, then C: `RPUSH f 1 2` | A gets `1`, B gets `2` |
| Blocked client on db 3 ignores db 0 | `blocked_pop_on_one_db_ignores_pushes_on_another` | A: `redis-cli -n 3 BLPOP q 0`. B: `redis-cli RPUSH q x` | A keeps waiting |
| Many clients at once (multithreaded) | `many_connections_at_once`, `conservation_under_stress` | `redis-benchmark -t set,get,lpush -c 100 -n 100000 -q` | No errors |
| Linearizable | `random_histories_are_linearizable`, `read_after_acknowledged_write_from_another_client`, `no_lost_wakeups` | `cargo test --release random_histories -- --nocapture` | 250 histories, all linearizable |
| Other machines can connect | `default_bind_accepts_connections_from_the_network` | See [Connecting from another machine](#connecting-from-another-machine) | `PONG` from the other machine |

### 3. From another machine

Start the server on machine A (`cargo run --release`). Find A's address (`ipconfig getifaddr en0` on macOS, `hostname -I` on Linux). On machine B:

```bash
redis-cli -h <A's address> -p 6379 PING                  # PONG
redis-cli -h <A's address> -p 6379 -n 4 BLPOP remote 0   # waits...
```

Then on machine A, run `redis-cli -n 4 RPUSH remote hello`. Machine B should print `remote`, `hello`. That shows remote clients and local clients share the same databases and blocking queues.

You can also run a whole benchmark across the network from machine B: `redis-benchmark -h <A's address> -t set,get -c 50 -n 100000 -q`.

---

## Demo script

About 5 minutes, with three terminals and a helper.

```bash
# T1 — start, with commands scrolling by
RUST_LOG=debug cargo run --release -- --port 7000

# Databases are independent
redis-cli -p 7000 -n 2 SET color blue        # helper
redis-cli -p 7000 GET color                  # (nil)
redis-cli -p 7000 -n 2 GET color             # "blue"

# FIFO: you block first, then the helper
redis-cli -p 7000 BLPOP q 0                  # you
redis-cli -p 7000 BLPOP q 0                  # helper
redis-cli -p 7000 RPUSH q one                # T3 → you get "one"
redis-cli -p 7000 RPUSH q two                # T3 → helper gets "two"

# Variadic push is applied whole before serving
redis-cli -p 7000 BLPOP foo 0                # waits
redis-cli -p 7000 LPUSH foo a b c            # waiter gets "c" (list is c b a)

# Multi-key and timeout
redis-cli -p 7000 BLPOP a b c 0              # then: redis-cli -p 7000 RPUSH c x
redis-cli -p 7000 BLPOP empty 1.5            # (nil) after 1.5 s

# SELECT rule (interactive)
redis-cli -p 7000                            # then type: GET x   then: SELECT 3  → error

# Crawler-style atomic script and transaction
redis-cli -p 7000 RPUSH frontier u1 u2
redis-cli -p 7000 EVAL "local u = redis.call('LPOP', KEYS[1]); redis.call('SADD', KEYS[2], u); return u" 2 frontier taken
redis-cli -p 7000                            # then: MULTI / INCR n / HSET h f v / EXEC

# Live stress + linearizability evidence
cargo test --release conservation -- --nocapture
cargo test --release random_histories -- --nocapture
```
