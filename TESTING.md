# Why the tests are enough

This document argues that the test suite shows micro-redis meets every requirement of the Mini 2 spec. It also says plainly where tests stop and reasoning takes over.

Tests alone can never prove that concurrent code has no bugs: a race the tests didn't happen to hit stays invisible. So the argument has three layers, and each covers what the others can't:

1. **A design argument** (a proof sketch): if the server follows three rules, every history it can produce is linearizable. This covers every interleaving, including ones no test ever hits.
2. **Targeted tests** that check each rule, and each known way to break it, against the real running server over TCP.
3. **Evidence that the tests can actually catch mistakes**: three bugs planted on purpose, each caught, plus a byte-for-byte comparison with real Redis for every "behaves like Redis" requirement.

How to run everything is at the end.

---

## 1. Requirement → evidence

Every row names the tests that would fail if the requirement were broken. Test names can be run individually with `cargo test --release <name>`.

| Requirement (spec) | Tests that check it | What they check |
| --- | --- | --- |
| Redis-compatible service in Rust on tokio | `replies_match_real_redis` | Same bytes as a real `redis-server` for ~230 hand-written cases plus 5,000 random commands |
| Asynchronous and multithreaded | `many_connections_at_once`, `conservation_under_stress`, all of `tests/concurrency.rs` (run on 4–8 worker threads) | 300 simultaneous clients; 6 pushers and 6 blocked poppers at once; blocked clients don't stop others |
| Every request/response linearizable | `random_histories_are_linearizable`, `multi_database_histories_are_linearizable`, `read_after_acknowledged_write_from_another_client`, `readers_never_see_time_go_backwards`, plus section 3 | See section 2 |
| 16 independent databases (0–15) | `multi_database_histories_are_linearizable`, `select_isolates_databases`, `databases_are_independent`, `blocked_pop_on_one_db_ignores_pushes_on_another`, `flushall_clears_every_database` | A key in one database is invisible in another; a push on db 0 doesn't wake a waiter on db 3 |
| Default database is 0 | `database_0_is_the_default_without_select` | A client that never sends `SELECT` reads and writes db 0 |
| `SELECT` only at the start of a session | `select_only_as_first_command`, `select_works_only_first`, `any_executed_command_closes_the_window`, `select_errors` | Allowed first; rejected after any executed command; out-of-range and non-numeric indexes are errors |
| RESP2 | `every_reply_type_is_exact`, `ping_byte_by_byte`, `binary_safe_values`, `every_prefix_of_a_valid_frame_is_incomplete`, `random_bytes_never_panic`, `malformed_resp_gets_an_error_then_close`, `oversized_frame_is_never_allocated` | Exact bytes for all five reply types; requests split at every byte; binary values; garbage and huge lengths handled safely |
| Port 6379 by default, configurable; reachable from other machines | `default_bind_accepts_connections_from_the_network`, `defaults`, `flags`, `server_binary_honours_port_flag_and_cli_binary_talks_to_it`, `server_binary_rejects_bad_flags` | Starts the real binary with `--port` and talks to it |
| `PING` (exempt from linearizability) | `ping_with_message`, `ping_byte_by_byte` | `PONG` and `PING msg` |
| `GET`, `SET` | `set_get_overwrite`, `set_replaces_a_list`, `strings_and_wrongtype` | Overwrite, replacing another type, nil for a missing key |
| `EXISTS` | `exists_counts_repeats` | A key named twice counts twice, as in Redis |
| `LPUSH`, `RPUSH` | `push_returns_length_and_lpush_reverses`, `push_order_and_lengths`, `wrongtype_errors` | Variadic, returns the new length, `LPUSH a b c` gives `c b a`, `WRONGTYPE` on strings |
| `BLPOP`, `BRPOP` | `blpop_takes_first_non_empty_key_without_blocking`, `brpop_pops_from_the_tail`, `blpop_times_out_with_nil_after_the_timeout`, all of section 3 | Keys scanned left to right; tail vs head; nil after the timeout |
| At most 5 keys | `errors_keep_the_connection_usable`, `blocking_pop_validation` | 6 keys gives `ERR BRPOP accepts at most 5 keys` |
| Timeout is a double | `blpop_times_out_with_nil_after_the_timeout` (0.2 s), `blocking_pop_validation` | Fractional seconds work; negative, non-numeric and `NaN` are rejected; `0` means forever |
| Extensive programmatic tests, `cargo test` | The whole suite: 100 tests | |
| Optional: linearizability checker | `random_histories_are_linearizable`, `multi_database_histories_are_linearizable`, `checker_rejects_impossible_histories` | Section 2 |

---

## 2. Linearizability

### What has to be true

A history is linearizable if every operation can be given one instant, its *linearization point*, that lies between when the client sent the request and when it got the reply. Running all operations one by one in the order of those instants, on a plain single-threaded map, must produce exactly the replies the clients saw.

### The design argument (covers every interleaving)

Each database is one `Mutex<State>`. The server follows three rules:

1. **One critical section per command.** Every command (a whole `MULTI`/`EXEC` and a whole Lua script count as one command) does all its reads and writes while holding its database's lock, and never `.await`s while holding it.
2. **Reply after the lock is released.** The reply is written only after the critical section has ended.
3. **Blocked clients are served inside the push.** A push hands elements to waiting `BLPOP`/`BRPOP` clients before releasing the lock. A blocking pop checks the lists and registers itself as a waiter in *one* critical section.

From these, the proof is short:

- By rule 1, critical sections on one database never overlap. So they happen in a single order, and each command sees exactly the state left by the one before it. That order is a sequential execution.
- Pick any instant inside a command's critical section as its linearization point. The client sent the request before the server could start the critical section, and by rule 2 the reply leaves after it ends. So the point lies inside the request/reply interval.
- Different databases share no data, so each database's order can be checked on its own, and together they form one valid order.
- **Blocking pops.** A served `BLPOP` gets its linearization point right after the push that served it, inside that same critical section (rule 3), so nothing can slip between them. A `BLPOP` that times out gets its point at the moment it registered, when every key was empty. By rule 3 no push can have landed on those keys without serving it, so it is correct that it found nothing. `cancel` on timeout or disconnect also runs under the lock, so "served" and "timed out" can never both happen.

### The tests that check the rules hold in the real server

The argument is only as good as the code's obedience to the rules. These tests attack each rule from outside, over TCP:

| Rule | Broken how | Caught by |
| --- | --- | --- |
| 1 and 2 | Reply sent before the write is visible to everyone | `read_after_acknowledged_write_from_another_client`: after client A sees `OK`, client B must see the value. `readers_never_see_time_go_backwards`: once a reader sees a new value, no later reader sees an older one |
| 1 | Two commands interleave inside one database | `transactions_are_atomic_under_concurrency`, `scripts_are_atomic_no_lost_updates` (16 clients, no lost increments), `crawler_style_claim_hands_each_url_out_once` |
| 3 | Check and register in two steps (lost wake-up) | `no_lost_wakeups` (300 races) |
| 3 | Push releases the lock before serving | `waiting_client_is_never_overtaken_by_a_newcomer` (40 rounds) |
| All | Anything at all | `random_histories_are_linearizable`, `multi_database_histories_are_linearizable` (below) |

### The general checker

`random_histories_are_linearizable` doesn't look for a specific bug. It checks the definition directly:

- 4 clients each run 5 random operations at the same time: `SET`, `GET`, `EXISTS`, `LPUSH`, `RPUSH`, `BLPOP` and `BRPOP` with short timeouts, on a few shared keys, including type changes that produce `WRONGTYPE`.
- Each operation records when it was sent and when its reply arrived.
- A Wing & Gong search tries every order that respects real time (if A finished before B started, A comes first) and replays it on a simple sequential model. If no order reproduces every observed reply, the test fails and prints the history.
- 250 histories per run by default; `STRESS_ITERS=2000` runs 2,000.

`multi_database_histories_are_linearizable` does the same with bigger histories spread over databases at once: 6 clients × 5 operations, two clients each on databases 0, 7 and 15, and the database-0 pair never sends `SELECT`. Every database uses the same key names, and the model keeps one keyspace per database. So the check fails if a value, a push or a wake-up ever crosses from one database to another, as well as for any ordering problem inside a database. 150 histories per run by default.

**Why the checker can be trusted:**

- **No false alarms.** The recorded send/receive times are *outside* the server's real ones (taken just before sending and just after receiving), so the true intervals sit inside the recorded ones. Any order valid on the true intervals is valid on the recorded ones, so a correct server can never be reported as wrong.
- **Not a rubber stamp.** `checker_rejects_impossible_histories` feeds it histories that are *not* linearizable: a stale read after a completed write, the same pushed value delivered to two poppers, a timed-out `BLPOP` while a value sat in the list, a `GET` on database 3 seeing a value set on database 0, and a `BLPOP` on database 3 served by a push on database 0. It must reject every one, while still accepting the same operations when they overlap in time, where either order is legal. A checker that accepted everything would make the random test meaningless.
- **Exhaustive for each history.** It searches every valid order with memoization, so "linearizable" is a fact about the recorded history, not a heuristic guess.

---

## 3. Blocking commands: every race, one test each

Blocking pops are where concurrent Redis servers usually go wrong, so each specific race has its own test, and most repeat hundreds of times:

| Race / property | Test | Rounds (default) |
| --- | --- | --- |
| Push lands between "lists are empty" and "register as waiter" (lost wake-up; push timing swept across 0–2.25 ms) | `no_lost_wakeups` | 300 |
| A newcomer's `BLPOP` takes the element ahead of a client already waiting (the newcomer retries non-stop throughout the push) | `waiting_client_is_never_overtaken_by_a_newcomer` | 40 |
| Oldest waiter first (FIFO) | `waiters_are_served_first_come_first_served`, `push_hands_off_fifo_inside_the_push` | |
| `LPUSH k a b c` is applied whole before anyone is served (waiter gets `c`) | `variadic_push_is_applied_whole_before_serving`, `variadic_lpush_serves_after_the_whole_push` | |
| One push serves several waiters | `one_push_can_serve_several_waiters` | |
| A waiter on several keys gets exactly one value | `multi_key_waiter_gets_exactly_one_value`, `multi_key_waiter_is_served_exactly_once`, `duplicate_keys_queue_once` | 50 |
| Timeout fires at the same moment as a push: exactly one wins | `deadline_vs_push_is_decided_exactly_once`, `cancel_vs_serve_exactly_one_wins` | 300 |
| Client hangs up at the same moment as a push: value delivered, kept, or counted as dropped, never two of these, never none | `disconnect_vs_push_is_decided_exactly_once`, `dead_receiver_puts_value_back_and_next_waiter_gets_it` | 200 |
| A blocked client doesn't hold up commands it sent after | `commands_after_a_blocking_pop_wait_their_turn` | |
| Everything at once | `conservation_under_stress` | 3 runs |

`conservation_under_stress` is the catch-all. 6 pushers each push 300 unique values while 6 poppers run `BLPOP` and `BRPOP` on random key subsets, with random short timeouts and some clients hanging up mid-wait. At the end, every value must be in exactly one place:

```
received + still in the lists + lost with a disconnected client == pushed
```

and no value may be received twice. A lost wake-up, double delivery, or value vanishing in a race would break the equation.

Invariants are also checked after the concurrency tests (`server.check_invariants()`). In particular, no waiter may ever be queued on a key that holds a non-empty list. That invariant is what makes a timed-out `BLPOP`'s `nil` correct.

---

## 4. "Behaves like Redis": differential testing

The spec says `GET`, `SET`, `EXISTS`, `PING`, the pushes and the blocking pops behave as in Redis. Instead of trusting my reading of the Redis docs, `replies_match_real_redis` starts a real `redis-server` and sends both servers the same commands in lockstep. Every reply must be byte-identical, error text included.

- About 230 hand-written edge cases: wrong argument counts, wrong types, empty lists, negative and fractional timeouts, missing keys, and more.
- Then 5,000 random commands by default (`STRESS_ITERS=50000` for more), drawn from 35 command types over a few shared keys, so types collide and lists empty out. They include `BLPOP` and `BRPOP` on 1 to 3 keys. With a 10 ms timeout, both servers either pop at once or wait and reply nil, so the comparison stays in lockstep. Now and then the timeout is invalid (`-1`, `abc`), so those error replies are compared too.
- After blocking pops were added, 4 seeds × 20,000 commands all matched.
- Before release it ran 50,000 commands on 4 seeds. That longer run found two real bugs (`LINDEX`/`LSET` must look up the key before parsing the index, and `RPOPLPUSH k k` on a one-element list dropped the key's TTL). Both are fixed and kept as regression cases.

Where the spec deliberately differs from Redis (`SELECT` only first, at most 5 keys), the spec's behaviour is tested directly instead (section 1).

---

## 5. Evidence the tests can catch mistakes

A test suite that passes might simply be too weak to fail. To show it isn't, the code contains three deliberate bugs, each behind a cargo feature and each breaking exactly one of the rules in section 2:

| Feature | Bug it plants | Rule broken | Caught by |
| --- | --- | --- | --- |
| `bug-reply-before-apply` | `SET` replies `OK`, then applies the write 5 ms later | 2 | `read_after_acknowledged_write_from_another_client`, `random_histories_are_linearizable`, `multi_database_histories_are_linearizable`, plus the comparison with Redis and several others |
| `bug-split-register` | `BLPOP` checks the lists and registers as a waiter in two separate critical sections | 3 | `no_lost_wakeups`, `random_histories_are_linearizable`, `multi_database_histories_are_linearizable` |
| `bug-split-serve` | A push releases the lock before serving waiters | 3 | `waiting_client_is_never_overtaken_by_a_newcomer` |

```bash
cargo test --release --no-fail-fast --features bug-reply-before-apply   # must fail
cargo test --release --no-fail-fast --features bug-split-register       # must fail
cargo test --release --no-fail-fast --features bug-split-serve          # must fail
```

Each bug is the classic way a concurrent Redis gets linearizability wrong. Every one is caught, and the two general checkers catch two of the three without being written for them. So passing the suite means something.

**Catching them must not depend on luck.** A race test that fires at one guessed moment can miss a bug on a machine with different timing. That happened once on macOS: `waiting_client_is_never_overtaken_by_a_newcomer` sent its newcomer's `BLPOP` at one fixed moment, and on one run it missed `bug-split-serve`. Now the race tests cover the whole race instead of guessing:

- **`waiting_client_is_never_overtaken_by_a_newcomer`:** the newcomer sends very short `BLPOP`s non-stop. The push only starts once the newcomer has made a few attempts, and the newcomer stops only after a few more attempts once the push has answered. So its attempts cover the whole push.
- **`no_lost_wakeups`:** sweeps the push across 0 to 2.25 ms after the `BLPOP`.

Measured on a machine with every core kept busy by other programs, to scramble the timing:

| | Result |
| --- | --- |
| `bug-split-serve` | caught in 20 of 20 runs |
| `bug-split-register` | caught in 20 of 20 runs |
| Correct server | failed 0 of 30 runs |

---

## 6. Repetition

Races are random, so one passing run is weak evidence. The race tests take `STRESS_ITERS` to repeat more:

```bash
STRESS_ITERS=2000 cargo test --release --test concurrency
STRESS_ITERS=2000 cargo test --release --test linearizability
COMPAT_SEED=7 STRESS_ITERS=50000 cargo test --release --test compat
for i in $(seq 1 10); do cargo test --release -q || break; done
```

All of these pass, including under macOS's connection limits (a 128-connection accept queue and about 16,000 local ports). That was checked by replaying the sequence with those limits set.

---

## 7. What tests can't show, and how it's covered

- **Interleavings no test hit.** Covered by the design argument in section 2, which holds for every interleaving. The tests then confirm the code follows the rules the argument depends on, and section 5 shows they would notice if it didn't.
- **The checker's history size.** Exhaustive search limits each history to 20–30 operations. Small histories still exercise every race above, because each needs only two or three operations, and thousands of them run.
- **A panic inside a critical section** would leave a database half-updated. The build sets `panic = "abort"`, so the service stops instead of serving inconsistent data, which the spec allows ("only bugs in your own code can cause problems").
- **New tests were checked against breakage too.** Each new test was run against a deliberately broken server before being kept. The multi-database checker failed on its first history when `SELECT` was made to answer `OK` without switching databases. The network test failed when the default bind was put back to `127.0.0.1`.
- **Not covered by tests:** long soak runs (hours, watching memory), and Redis's own test suite. Redis's suite moves between databases with `SELECT` mid-session, which this spec forbids.
- **Timing tests on a loaded machine.** Tests that wait for a client to block poll the server's own waiter count (`wait_until`) instead of sleeping, so a slow machine makes them slower, not wrong.

---

## 8. Running it all

```bash
cargo fmt --check
cargo clippy --all-targets -- -D warnings
cargo test --release                          # 100 tests, all must pass
# then sections 5 and 6
```

`tests/compat.rs` needs `redis-server` on the `PATH` (`brew install redis` or `sudo apt install redis-server`). Without it, that one test prints `SKIPPED` and passes.

For checking each requirement by hand with `redis-cli`, see "Checking every requirement" in `README.md`.
