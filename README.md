# Topic 29 - Distributed Task Notification and Worker System

This is a small TCP + Mininet project for Topic 29.

## What it demonstrates

- TCP clients submit tasks to one server.
- Three workers register, receive one task at a time, and send a completion message.
- The server tracks each worker as `idle`, `busy`, or `offline`.
- If a worker disconnects or misses heartbeats, its unfinished task returns to the queue.
- `adaptive` scheduling chooses the worker with fewer completed tasks. `round_robin` is the baseline.
- When a worker fails, `sdn_manager.py` adds an OpenFlow drop rule for that worker IP. This is the socket-to-SDN link.

A task is deliberately simple: it only has an ID and a delay in seconds. This keeps the networking behavior easy to explain.

## Files

- `server.py` - TCP task queue and scheduler
- `worker.py` - worker registration, heartbeat, task execution
- `client.py` - sends one task
- `topology.py` - one switch, eight hosts, and an automatic demonstration
- `sdn_manager.py` - converts a failed-worker event into an OpenFlow rule
- `analyze_metrics.py` - prints only completed tasks, requeues, and average completion time

## Topology

```
 h2-h5 clients        h6-h8 workers
       \                 /
        \               /
              s1
               |
              h1
           TCP server
```

`h6` runs worker1 and intentionally stops during a long task. The server requeues that task to worker2 or worker3.

## Ubuntu / Mininet setup

```bash
sudo apt update
sudo apt install mininet openvswitch-switch -y
sudo mn -c
```

Copy this project into the VM shared folder, open that folder in the Ubuntu terminal, then run:

```bash
sudo python3 topology.py
```

Wait about 12 seconds. At the `mininet>` prompt:

```bash
h1 cat server.log
h1 cat metrics.csv
h6 cat worker1.log
h7 cat worker2.log
sh cat sdn_manager.log
sh ovs-ofctl -O OpenFlow13 dump-flows s1
```

You should see worker1 fail, one `requeued` event, all four tasks finish, and a priority-100 drop rule for `10.0.0.6`.

## Small baseline comparison

Run once with the default adaptive scheduler. Then change this one line in `topology.py`:

```python
h[1].cmd("python3 server.py --scheduler adaptive > server.log 2>&1 &")
```

to:

```python
h[1].cmd("python3 server.py --scheduler round_robin > server.log 2>&1 &")
```

Run the same four task delays (6, 2, 1, 3) and compare the three values printed by:

```bash
python3 analyze_metrics.py metrics.csv
```

## Viva explanation

- TCP is used because task delivery and completion messages must be reliable.
- Heartbeats detect a silent worker; a closed TCP connection detects a crashed worker.
- A failed task is put back into the queue and sent only to an idle healthy worker.
- The application writes a failure event. The SDN manager reads it and installs a drop rule on `s1` for that failed worker.
- Adaptive scheduling is compared with round robin using the same tasks.

## Limits

This is a classroom prototype. It has one switch, fake tasks, and direct `ovs-ofctl` rules instead of a full controller framework. Its SDN behavior is intentionally limited to isolating a failed worker.
