# Topic 29 - Distributed Task Notification and Worker System (TCP + SDN)

Jackfruit SDN-Socket Programming mini project. Clients send tasks to a TCP server, the server
hands them to registered workers, recovers from worker failures, and tells an **SDN manager**
(also over TCP) so the network reacts to what the application sees.
**Every message in the system is TCP.**

## 1. Requirements -> implementation

| Project requirement | Where |
| --- | --- |
| TCP task communication | `server.py`, `worker.py`, `client.py` (raw `socket`, newline-delimited JSON, port 5000, one thread per connection) |
| Register worker nodes | `register` message (name + speed); server keeps a table of workers |
| Assign tasks | `choose_idle_worker()` - `adaptive` (fastest idle worker) or `round_robin` (baseline) |
| Track worker workload | per worker: state (`idle`/`busy`/`slow`/`offline`), assigned, completed; `status` message |
| Handle task timeouts | task running longer than `delay/speed + 3 s` is requeued; the slow worker is parked as `slow` (not blocked) until it reports back |
| Detect worker failures | connection closed **or** no heartbeat for 3 s (covers crash and hang) |
| Redistribute tasks | the failed worker's task returns to the queue with `attempt+1`; late results from the old attempt are ignored |
| Evaluate scheduling efficiency | `analyze_metrics.py`, `run_all.sh`, results in section 7 |
| SDN driven by the application | `sdn_manager.py` receives TCP events and installs / removes OpenFlow rules on `s1` |

## 2. Architecture

```
            root namespace                              Mininet
        +-------------------+  TCP 9999        h1 server (10.0.0.1)  <-- TCP 5000 -- h2..h5 clients
        |   sdn_manager.py  | <--------------- server.py                          \
        |  10.0.0.254       |   events                                             s1 (OpenFlow 1.3, OVS)
        +---------+---------+                  h6, h7, h8 workers  --- TCP 5000 --/
                  |  ovs-ofctl add-flow / del-flows (OpenFlow rules)       |
                  +--------------------------------------------------------+
```

| Host | Role | Notes |
| --- | --- | --- |
| h1 `10.0.0.1` | TCP server / scheduler | port 5000 |
| h2-h5 | clients | submit delayed tasks |
| h6, h7, h8 | worker1, worker2, worker3 | speeds 2.0 / 1.0 / 0.5 (task time = delay / speed) |
| root `10.0.0.254` | SDN manager | linked to `s1` so the server can reach it over TCP port 9999 |

Tasks are simulated: a task is an ID and a delay in seconds.

## 3. Protocol (newline-delimited JSON over TCP)

| Direction | Message |
| --- | --- |
| worker -> server | `register{worker,speed}`, `heartbeat{worker}` (every 1 s), `done{worker,task_id,attempt}` |
| server -> worker | `registered`, `task{task_id,delay,attempt}` |
| client -> server | `submit{task_id,delay}`, `status` |
| server -> client | `queued`, `status`, `error` |
| server -> SDN manager (TCP 9999) | `{event, worker_ip}` with `worker_failed`, `worker_registered`, `worker_busy`, `worker_idle` |

## 4. SDN design

`sdn_manager.py` is a small control process. It listens on TCP, keeps the state of the workers, and
programs the OpenFlow switch `s1` through `ovs-ofctl -O OpenFlow13` (the switch has no external
OpenFlow controller; a priority-0 `NORMAL` rule gives plain forwarding).

| Event from the application | SDN action |
| --- | --- |
| `worker_failed` (crash or heartbeat lost) | **Quarantine:** `priority=100` drop rules for IP traffic to and from that worker, `hard_timeout` 8 s |
| `worker_registered` | remove the drop rules (the worker is back) |
| `worker_busy` / `worker_idle` | workload tracking, logged as `LOAD <ip> busy/idle` |
| every 2 s (monitoring) | `dump-ports s1` appended to `port_stats.log` |

A **task timeout does not block the worker**: it is slow, not dead.
The quarantine expires by itself, so a restarted worker can reconnect and register again.
Application events change the flow table, and the flow table (drop rules) changes what the
application sees (the failed worker cannot reach the server until the quarantine ends).

## 5. Setup and run

Ubuntu VM with Mininet and Open vSwitch (see the Mininet installation guide):

```bash
sudo apt update && sudo apt install mininet openvswitch-switch -y
sudo mn -c
```

**Demo (interactive):**

```bash
sudo python3 topology.py --quarantine 20          # default scenario: failure, adaptive scheduler
# wait ~35 s, then at the mininet> prompt:
h1 cat server.log
sh cat sdn_manager.log
sh ovs-ofctl -O OpenFlow13 dump-flows s1          # drop rules are visible during the quarantine
sh python3 analyze_metrics.py metrics.csv
```

Other options: `--scheduler round_robin`, `--scenario burst|light|failure`, `--no-cli`.

**Full evaluation in Mininet** (3 scenarios x 2 schedulers, about 4 minutes):

```bash
sudo ./run_all.sh            # writes results/summary.csv and per-run metrics/SDN logs
```

**Tests without Mininet** (loopback, SDN manager in `--dry-run`; 21 checks and the same scenarios):

```bash
python3 local_test.py          # use --quick to skip the scenario runs
```

## 6. Test cases

| # | Test | Expected | Where |
| --- | --- | --- | --- |
| T1 | Task queued before the worker registers | task completes, worker not failed | `local_test.py` |
| T2 | Task exceeds its timeout | `task_timeout`, finished by another worker, **no** SDN block | `local_test.py` |
| T3 | Worker crashes mid-task, restarts later | `worker_failed`, `requeued`, `BLOCK` in SDN log, task done once, worker registers again, quarantine ends | `local_test.py` |
| T4 | Worker hangs (`SIGSTOP`) then resumes | heartbeat loss detected, task done by another worker, `worker_recovered` | `local_test.py` |
| T5 | Three scenarios x two schedulers | all 20 tasks complete in every run | `local_test.py`, `run_all.sh` |
| T6 | Mininet: `dump-flows s1` during quarantine | `priority=100 ... actions=drop` for the failed worker IP | manual demo |
| T7 | Mininet: `h6 ping -c1 h1` before the demo | connectivity works | manual |

## 7. Performance evaluation

Workers: speed 2.0, 1.0, 0.5. Four clients x 5 tasks of 2 s (20 tasks). Baseline = `round_robin`,
proposed = `adaptive` (fastest idle worker, ties -> fewer completed tasks).

| Scenario | Load |
| --- | --- |
| `burst` | 4 clients start together, one task every 3 s (workers saturated) |
| `light` | clients start 1.5 s apart, one task every 6 s each (one task per 1.5 s overall) |
| `failure` | `light` load; worker1 crashes half-way through its 3rd task and restarts 5 s later |

**Reference results (loopback run of `local_test.py`, no Mininet).** Replace with the values from
`results/summary.csv` produced by `sudo ./run_all.sh`.

| Scenario | Scheduler | Avg completion (s) | p95 (s) | Makespan (s) | Requeued | Tasks per worker |
| --- | --- | --- | --- | --- | --- | --- |
| burst | adaptive | 2.40 | 5.00 | 17.01 | 0 | w1:10 w2:6 w3:4 |
| burst | round_robin | 2.41 | 5.00 | 17.00 | 0 | w1:10 w2:6 w3:4 |
| light | adaptive | 1.00 | 1.01 | 29.51 | 0 | w1:20 |
| light | round_robin | 2.25 | 4.00 | 30.51 | 0 | w1:7 w2:7 w3:6 |
| failure | adaptive | 1.35 | 4.00 | 29.50 | 1 | w1:16 w2:3 w3:1 |
| failure | round_robin | 2.38 | 4.00 | 31.01 | 1 | w1:6 w2:8 w3:6 |

Reading the results:
- When every worker is already busy (`burst`) both schedulers behave the same: there is no choice to make.
- When load is below capacity (`light`, `failure`) `adaptive` sends work to the fastest idle worker, so average completion time is about 2.2x (light) and 1.8x (failure) lower than `round_robin`.
- Both schedulers complete all 20 tasks after a crash: the task is requeued once and the worker returns after its quarantine.
- The price of `adaptive`: at light load it uses only the fastest worker (`w1:20`), so the work is not spread out.

## 8. Limitations

- One server (single point of failure); one switch; tasks are simulated delays.
- The SDN manager is a control process that programs OVS with `ovs-ofctl`, not an OpenFlow controller speaking the protocol. Its only network action is the failure quarantine; busy/idle events are tracked and logged but do not change flows.
- Mininet numbers may differ slightly from the loopback reference; use your own `results/summary.csv`.

## 9. Files

`server.py` `worker.py` `client.py` - TCP application | `sdn_manager.py` - SDN manager |
`topology.py` - Mininet topology and scenarios | `run_all.sh` - Mininet evaluation |
`analyze_metrics.py` - metrics summary | `local_test.py` - tests without Mininet |
`results/summary_local.csv` - reference results | `ADMIN_README.md` - team guide.
