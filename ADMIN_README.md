# Admin Guide - Topic 29 Worker System (branch `sub3`)

For the project team: read before the Mininet demo and the viva.

## 1. Project in one sentence

Clients send delayed tasks over TCP to a server; the server assigns them to workers, requeues a task
when its worker fails or times out, and tells the SDN manager (over TCP) so the switch quarantines the
failed worker with OpenFlow drop rules.

## 2. Team roles during the demo

| Person | Responsibility |
| --- | --- |
| Admin 1 | Starts Mininet, explains topology (`h1` server, `h2-h5` clients, `h6-h8` workers, root = SDN manager) |
| Admin 2 | Shows `server.log`, explains TCP, heartbeat, requeue, timeout, adaptive vs round robin |
| Admin 3 | Shows `sdn_manager.log` and `dump-flows`, explains the quarantine and the TCP event channel |

Everyone must understand the whole flow.

## 3. Components

| File | Purpose |
| --- | --- |
| `server.py` | TCP server: queue, scheduler, heartbeat monitor, timeouts, failure recovery, sends events to the SDN manager |
| `worker.py` | Registers (name + speed), heartbeats, runs one task at a time. Test flags: `--fail-on-task N`, `--fail-after S`, `--restart-after S`, `--slowdown F` |
| `client.py` | Submits tasks: `--tasks N --interval S --start-delay S` |
| `sdn_manager.py` | TCP server on port 9999; installs / removes OpenFlow drop rules with `ovs-ofctl`; samples port stats |
| `topology.py` | Mininet: 1 switch, 8 hosts, root-namespace link for the SDN manager; scenarios `burst`, `light`, `failure` |
| `run_all.sh` | Runs 3 scenarios x 2 schedulers in Mininet, writes `results/summary.csv` |
| `analyze_metrics.py` | Completed tasks, requeues, avg / p95 completion, makespan, tasks per worker |
| `local_test.py` | 21 checks without Mininet (loopback, SDN manager in dry-run) |

## 4. Demo flow (`sudo python3 topology.py --quarantine 20`)

1. SDN manager starts (TCP 9999), then the server on `h1`, then workers (speeds 2.0 / 1.0 / 0.5), then the clients.
2. `h2-h5` submit 20 tasks. The scheduler gives each task to the fastest idle worker.
3. `worker1` crashes half-way through its 3rd task. The server sees the closed connection, marks it `offline`, requeues the task (`attempt=2`).
4. The server sends `worker_failed` to the SDN manager over TCP; the manager installs `priority=100` drop rules for `10.0.0.6`.
5. Another worker finishes the requeued task. After the quarantine the rules expire, `worker1` restarts, registers again and gets tasks.

At the `mininet>` prompt (about 5-15 s after the crash the drop rules are visible):

```bash
sh ovs-ofctl -O OpenFlow13 dump-flows s1
h1 cat server.log
sh cat sdn_manager.log
sh python3 analyze_metrics.py metrics.csv
```

## 5. What the output proves

| Evidence | Meaning |
| --- | --- |
| `registered ... speed=` x3 | Workers connected over TCP |
| `queued` x20, `assigned`, `done` | Client -> server -> worker -> server communication works |
| `worker_failed ... connection closed` + `requeued` | Failure detection and redistribution |
| `sdn_notification ... worker_failed` | Server told the SDN manager over TCP |
| `BLOCK 10.0.0.6 for 20s` in `sdn_manager.log` | Manager reacted |
| `priority=100,ip,nw_src=10.0.0.6 ... actions=drop` | OpenFlow rule installed on the switch |
| `QUARANTINE ENDED` / second `registered` for worker1 | Recovery works |
| `LOAD 10.0.0.x busy/idle` | Workload reaches the SDN side |

## 6. Evaluation: what to run and report

```bash
sudo ./run_all.sh          # ~4 minutes, writes results/summary.csv
```

Paste the table into section 7 of `README.md` (replace the loopback reference rows). Do not invent numbers.
Be ready to explain: `burst` is identical for both schedulers (all workers busy, no choice to make);
`light` and `failure` favour `adaptive` because it picks the fastest idle worker; the cost is that it
concentrates work on the fastest worker.

## 7. Simple viva answers

**Why TCP?** Ordered, reliable task and result delivery. A closed connection is also a failure signal.

**Why heartbeats and not only connection loss?** A hung process keeps its connection open. Silence for 3 s reveals it. (Test T4 uses `SIGSTOP`.)

**What is a task timeout and how is it different from failure?** The task took longer than `delay/speed + 3 s`. The task is requeued, but the worker is only marked `slow` (not blocked) until it reports back.

**How are duplicate results avoided?** Every assignment has an attempt number. A result is accepted only from the worker currently holding that attempt.

**What is adaptive scheduling?** Choose the idle worker with the highest speed (ties: fewest completed). The baseline is round robin.

**What is the SDN part?** The server sends events to `sdn_manager.py` over TCP. On `worker_failed` the manager installs OpenFlow drop rules on `s1` (quarantine); on `worker_registered` it removes them. It also tracks busy/idle load and samples port counters.

**Is `sdn_manager.py` an OpenFlow controller?** No. It is a control process that programs the OpenFlow switch (Open vSwitch) through `ovs-ofctl`; there is no controller speaking the OpenFlow protocol. Say this honestly if asked.

**Why does the quarantine expire?** A blocked worker could never reconnect, so the drop rules have a `hard_timeout` (8 s by default). A restarted worker retries until the rules expire, then registers again.

**Why is the controller reachable by TCP?** The root namespace is linked to `s1` with IP `10.0.0.254`; the server connects to `10.0.0.254:9999`.

**What are the limits?** Single server, one switch, simulated tasks, the SDN action is the failure quarantine only (busy/idle is logged, it does not change flows).

## 8. If the demo fails

1. Exit Mininet and run `sudo mn -c`.
2. `sudo mn` then `pingall` then `exit` to check Mininet itself.
3. Check `python3 --version` and `ovs-ofctl --version`.
4. Run `sudo python3 topology.py` again; check `sdn_manager.log` (does it say "listening on TCP 9999"?).
5. If hosts cannot reach the manager: `h1 ping -c1 10.0.0.254`.
6. Fall back to `python3 local_test.py` to show the logic works without Mininet.

## 9. Final checklist

- [ ] Branch `sub3` pushed to GitHub (and merged into `main` if this is the final version).
- [ ] The shared-folder / VM copy has the same files.
- [ ] `sudo ./run_all.sh` run once in the VM; `results/summary.csv` pasted into the README.
- [ ] `sudo mn -c` before the live demo.
- [ ] Screenshots: `dump-flows` with drop rules, `sdn_manager.log`, `server.log` showing `requeued`.
- [ ] Every team member can explain sections 4 and 7.
