# Distributed Task Notification & Worker System over TCP + SDN

Jackfruit Mini Project – SDN / Socket Programming. A TCP task-processing system (client → scheduler → workers)
running inside Mininet and controlled by a Ryu OpenFlow 1.3 controller. The SDN reacts to application events
(worker up/down, unauthorized hosts), prioritizes task traffic under congestion and is compared with a static baseline.

## 1. Problem and objectives
| Requirement | Where it is implemented |
|---|---|
| TCP task communication | `common.py` (framing), `scheduler.py`, `worker.py`, `client.py` (raw `socket`, thread per connection) |
| Register worker nodes | `REGISTER`/`REGISTERED`, token auth, re-registration replaces stale entry |
| Assign tasks | `Scheduler._pick_locked` / `_dispatch_loop` (policies: `round_robin`, `least_loaded`) |
| Track workload | per-worker `active` set, `capacity`, `speed`, heartbeat-reported load, `STATS` query |
| Task timeouts | `_monitor_loop`: task not finished in `--task-timeout` s → re-queued, old worker excluded, max-retries |
| Worker failure detection | connection loss **or** heartbeat silence > `--hb-timeout` (handles crash *and* hang) |
| Redistribute tasks | tasks of a dead worker are re-queued at the head; late duplicates are discarded (attempt numbers) |
| Evaluate scheduling | `run_experiments.py` + `analyze.py` (baseline vs proposed) |

## 2. Architecture
```
 root ns (Ryu controller, OF 6653, UDP notify 9999, IP 10.0.0.254)
        |  OpenFlow 1.3                         ^ UDP events: WORKER_UP/DOWN, LOAD, BLOCK
        v                                       |
 client(.2) sched(.1) bgc(.20) -- s1 ---- s2 -- w1(.11) w2(.12) bgs(.21)
                                     \
                                      s3 -- w3(.13) w4(.14) att(.66)
```
Message flow (newline-delimited JSON over TCP, port 5000):
```
worker -> sched : REGISTER{worker_id,capacity,speed,auth}   sched -> worker : REGISTERED | REJECT
worker -> sched : HEARTBEAT{load} (1 s)                      sched -> worker : TASK{task_id,attempt,duration,data}
worker -> sched : RESULT{task_id,attempt,exec_time,digest}
client -> sched : CLIENT_HELLO, SUBMIT{task_id,duration,data}, STATS     sched -> client : DONE | ERROR | STATS_REPLY
sched  -> SDN   : UDP JSON events (northbound notification)
```

## 3. SDN design (dynamic functionality)
All switch ports are limited to 10 Mbit/s with two OVS HTB queues (q0 best effort, q1 guaranteed 70 %).
| Mechanism | Trigger | Action |
|---|---|---|
| Application-aware prioritization | any TCP packet with port 5000 | per-flow rule (prio 30) `set_queue(1)` + output; background iperf stays in q0 |
| Worker-failure recovery | `WORKER_DOWN` from scheduler | flush all flows/MAC entries of that host on every switch (no stale rules) |
| Unauthorized-access blocking | `BLOCK` after 3 bad REGISTER/handshakes | drop rule (prio 200, `ipv4_src`) on **all** switches, 120 s hard timeout |
| Link-failure handling | `PortStatus` link-down | delete flows through the port, forget MACs learned there |
| Monitoring | every 2 s | port stats → `results/port_stats.csv`, `CONGESTION` events when tx ≥ 80 % |
| Mode switch (for evaluation) | `CONFIG{sdn_aware}` | `false` = plain learning switch (static baseline) |

Socket–SDN integration is therefore bidirectional in effect: application events (registration, failure, auth failure)
change flow tables, and flow tables (QoS queue, drops) change what the application experiences.

## 4. Setup
```bash
./setup.sh && . .venv/bin/activate     # mininet, openvswitch, iperf, ryu 4.34 (use Python 3.8/3.9), matplotlib
```

## 5. Run
**Unit/functional tests (no Mininet):** `python3 -m unittest tests.test_local -v`

**Manual demo**
```bash
# T1 (project root)
ryu-manager sdn_controller.py
# T2
sudo python3 topology.py        # Mininet CLI, then:
mininet> sched python3 scheduler.py --ctrl-host 10.0.0.254 --policy least_loaded > results/sched.log 2>&1 &
mininet> w1 python3 worker.py --id w1 --server 10.0.0.1 --speed 2.0 &
mininet> w2 python3 worker.py --id w2 --server 10.0.0.1 --speed 1.0 &
mininet> client python3 client.py --server 10.0.0.1 --tasks 100 --rate 30 --out results/manual.json
mininet> sh ovs-ofctl -O OpenFlow13 dump-flows s1          # shows prio-30 queue rules + L2 rules
mininet> w2 kill -STOP %1     # (or kill a worker) -> see re-distribution in results/sched.log
mininet> att python3 attacker.py --server 10.0.0.1         # blocked after 3 strikes
```
**Full automated evaluation**
```bash
# T1: ryu-manager sdn_controller.py        T2:
sudo python3 run_experiments.py --repeats 2
python3 analyze.py        # results/summary.csv + lat_p95_s.png, throughput_tps.png, makespan_s.png
```

## 6. Test cases
| # | Test | Expected |
|---|---|---|
| T1 | Workers register, tasks complete | all workers listed in `STATS`; 100 % tasks `DONE` |
| T2 | Heterogeneous workers, `least_loaded` | faster workers get more tasks, Jain index ≈ 1 |
| T3 | `round_robin` baseline | equal task counts regardless of speed |
| T4 | Worker killed mid-run | `worker_failures ≥ 1`, tasks re-queued, 100 % complete |
| T5 | Worker hangs (SIGSTOP) | detected by heartbeat timeout; tasks redistributed |
| T6 | Task exceeds timeout | `timeouts ≥ 1`, task finished by another worker |
| T7 | Wrong token | `REJECT`, not registered; after 3 strikes SDN drop rule installed (Mininet) |
| T8 | Background iperf congestion | task p95 latency lower with SDN QoS than baseline |
| T9 | `ping`/`pingAll` + `dump-flows` | connectivity + expected flow rules |
T1–T6 and the auth rejection part of T7 are automated in `tests/test_local.py`; T7 (blocking), T8, T9 run in Mininet.

## 7. Performance evaluation
Baseline = round-robin + plain L2 switch. Proposed = least-loaded + SDN-aware controller. Conditions
(`run_experiments.py`): `light_nobg`, `heavy_nobg`, `heavy_bg` (10 Mbit/s iperf competing), `failure_bg`
(worker w3 hangs at t=4 s), `attack` (unauthorized host at t=3 s). Metrics: makespan, throughput, mean/p95/max latency,
Jain fairness (normalized by capacity×speed), retried tasks, redistributed tasks, timeouts, detection delay, blocking.

Paste your measured `summary.csv` here and discuss it (expected trend: least-loaded wins when worker speeds differ;
QoS helps under background traffic; both modes recover from failures, SDN additionally cleans flows and blocks the attacker).

| condition | mode | throughput | p95 latency | makespan | ... |
|---|---|---|---|---|---|
| *fill from your run* | | | | | |

## 8. Files
`common.py scheduler.py worker.py client.py attacker.py` – TCP application · `sdn_controller.py` – Ryu app ·
`topology.py` – Mininet · `run_experiments.py`, `analyze.py` – evaluation · `tests/test_local.py` – tests ·
`setup.sh`, `requirements.txt`.

## 9. Viva cheat-sheet
* Why TCP? Reliable, ordered task/result delivery; connection loss itself is a failure signal.
* Why heartbeats *and* timeouts? Crash closes the socket; a hang does not – only silence reveals it. Task timeout catches slow/overloaded workers.
* How are duplicates avoided? Every assignment has an `attempt` number; results from an older attempt are ignored.
* Why UDP to the controller? Fire-and-forget, never blocks scheduling; controller state is soft and rebuilt from the next event.
* Why does QoS matter? Task payloads share links with bulk traffic; queue 1 guarantees bandwidth.
* Limitations: single scheduler (SPOF), tree topology (no loops → simple learning switch), simulated work, controller notify channel is unauthenticated (lab prototype).
