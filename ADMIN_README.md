# Admin Guide - Topic 29 Worker System

This document is for the project team. Read it before the Mininet demo and use it to explain the project to the faculty.

## 1. Project in one sentence

Clients send delayed tasks to a TCP server. The server assigns each task to an available worker. If a worker fails, the server requeues its unfinished task and the SDN manager blocks that failed worker IP on the OpenFlow switch.

## 2. Team roles during the demo

| Person | Suggested responsibility |
| --- | --- |
| Admin 1 | Starts Mininet and explains the topology. |
| Admin 2 | Shows `server.log` and explains TCP, workers, queue, and recovery. |
| Admin 3 | Shows `sdn_manager.log` and `dump-flows`, then explains the OpenFlow drop rule. |

Everyone should understand the full flow, not only their assigned part.

## 3. Components

| File | Purpose |
| --- | --- |
| `topology.py` | Creates one switch (`s1`) and eight Mininet hosts. It starts the server, three workers, and four clients. |
| `server.py` | TCP server, task queue, scheduler, heartbeat monitor, and failure recovery. |
| `worker.py` | Registers one worker, sends a heartbeat every second, performs a delayed task, and reports completion. |
| `client.py` | Sends one task to the server. |
| `sdn_manager.py` | Reads a failed-worker event and installs OpenFlow drop rules on `s1`. |
| `analyze_metrics.py` | Prints completed-task count, requeue count, and average completion time. |

## 4. Topology

```
 h2 h3 h4 h5 = clients
 h1             = TCP server
 h6 h7 h8       = worker1, worker2, worker3
 s1             = one OpenFlow switch
```

All hosts connect to `s1`. One switch is enough because this project demonstrates worker failure recovery, not alternate-path routing.

## 5. Demonstration flow

1. `h1` starts the TCP server.
2. `h6`, `h7`, and `h8` register as workers and send heartbeats.
3. `h2` to `h5` submit four tasks with delays 6, 2, 1, and 3 seconds.
4. The server gives the long 6-second task to worker1.
5. worker1 deliberately stops after 5 seconds.
6. The server sees the closed connection or missed heartbeat, marks worker1 offline, and requeues its unfinished task.
7. worker2 or worker3 finishes the requeued task.
8. The server writes the failure event to `sdn_events.jsonl`.
9. `sdn_manager.py` installs priority-100 OpenFlow drop rules for worker1 IP (`10.0.0.6`).

## 6. How to run in Ubuntu VM

Open the shared-folder project in an Ubuntu terminal and run:

```bash
sudo mn -c
sudo python3 topology.py
```

Wait about 12 seconds. At the `mininet>` prompt, run:

```bash
h1 cat server.log
h1 cat metrics.csv
h6 cat worker1.log
h7 cat worker2.log
sh cat sdn_manager.log
sh ovs-ofctl -O OpenFlow13 dump-flows s1
```

## 7. What successful output proves

| Evidence | Meaning |
| --- | --- |
| Three `registered` lines | Workers connected through TCP. |
| Four `queued` lines | Four clients submitted tasks. |
| `assigned` and `done` lines | Server-to-worker task communication works. |
| `requeued` line | An unfinished task returned to the queue. |
| `worker_failed` line | Failure detection works. |
| `SDN rule installed` line | Application failure triggered an SDN action. |
| `priority=100 ... actions=drop` | OpenFlow is blocking traffic to/from the failed worker. |

Run this from the shared-folder terminal after the demo:

```bash
python3 analyze_metrics.py metrics.csv
```

Expected minimum result: all four tasks completed and at least one task requeued.

## 8. Adaptive versus round robin

The default scheduler is `adaptive`. It chooses an idle worker with fewer completed tasks. The baseline is `round_robin`, which chooses idle workers in order.

For a small comparison, run the demo once with each scheduler using the same task delays. Change only this line in `topology.py`:

```python
h[1].cmd("python3 server.py --scheduler adaptive > server.log 2>&1 &")
```

to `round_robin`, rerun, and record only:

- completed tasks
- requeued tasks
- average completion time

Do not invent results. Use the values printed by `analyze_metrics.py`.

## 9. Simple viva answers

**Why TCP?** TCP reliably delivers ordered task and completion messages. Connection closure also helps detect a crashed worker.

**Why heartbeats?** A process can hang without closing its TCP connection. Missing heartbeats detect that case.

**How is duplicate completion avoided?** Each task has an attempt number. The server accepts a completion only from the worker currently assigned to that same attempt.

**What is the SDN integration?** The TCP application writes a failed-worker event. The SDN manager reads it and adds higher-priority OpenFlow drop rules for the failed worker. This proves an application event changes network behavior.

**Why use `NORMAL` flow?** There is no full controller framework in this simplified version. The normal rule provides basic single-switch forwarding, while the higher-priority drop rule overrides it for a failed worker.

**What are the limits?** Tasks are simulated delays, the topology has one switch, and the SDN action isolates a failed worker rather than rerouting through a second path.

## 10. If the demo fails

1. Exit Mininet and run `sudo mn -c` again.
2. Confirm Mininet itself works: run `sudo mn`, then `pingall`, then `exit`.
3. Confirm Python 3 and Open vSwitch are installed:

```bash
python3 --version
ovs-ofctl --version
```

4. Start the project again with `sudo python3 topology.py`.
5. If workers still cannot reach the server, run `h6 ping -c 1 h1` at the Mininet prompt. It must succeed before testing the Python programs.

## 11. Final checklist

- [ ] The GitHub `sub` branch has the simplified files.
- [ ] The shared-folder project has the same files.
- [ ] `sudo mn -c` is run before the live demo.
- [ ] The team has one successful adaptive run.
- [ ] The team has one round-robin baseline run.
- [ ] Screenshots or copied output show requeueing and the priority-100 drop rule.
- [ ] Every team member can explain Sections 5 and 9.
