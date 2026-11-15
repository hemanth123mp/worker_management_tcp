# mininet topology for topic 29: 1 switch, 8 hosts, sdn manager in the root namespace
#   h1 server | h2-h5 clients | h6-h8 workers (speed 2.0 / 1.0 / 0.5) | root = sdn manager 10.0.0.254
#
#   sudo python3 topology.py                              demo with the mininet cli
#   sudo python3 topology.py --scheduler round_robin      baseline scheduler
#   sudo python3 topology.py --quarantine 20              keep the drop rule longer for the demo
#   sudo python3 topology.py --scenario light --no-cli    automated run (burst | light | failure)

import argparse
import socket
import subprocess
import time
from pathlib import Path

from mininet.cli import CLI
from mininet.log import setLogLevel
from mininet.net import Mininet
from mininet.node import Node, OVSSwitch
from mininet.topo import Topo

SDN_IP = "10.0.0.254"
CLIENTS = {2: "client1", 3: "client2", 4: "client3", 5: "client4"}
WORKERS = {6: ("worker1", 2.0), 7: ("worker2", 1.0), 8: ("worker3", 0.5)}
# scenario -> (secs between a client's tasks, start offset between clients, crash worker1?)
SCENARIOS = {"burst": (3, 0, False), "light": (6, 1.5, False), "failure": (6, 1.5, True)}
TASKS_PER_CLIENT, TASK_DELAY = 5, 2
TOTAL_TASKS = len(CLIENTS) * TASKS_PER_CLIENT


class Topic29Topo(Topo):
    def build(self):
        # standalone = acts like a normal switch when no controller is attached
        sw = self.addSwitch("s1", protocols="OpenFlow13", failMode="standalone")
        for n in range(1, 9):
            self.addLink(self.addHost(f"h{n}", ip=f"10.0.0.{n}/24"), sw)


def wait_port(host, port, secs=5):
    end = time.time() + secs
    while time.time() < end:
        try:
            socket.create_connection((host, port), timeout=0.5).close()
            return True
        except OSError:
            time.sleep(0.2)
    return False


def count_done():
    try:
        return Path("metrics.csv").read_text().count(",done,")
    except FileNotFoundError:
        return 0


def run(args):
    net = Mininet(topo=Topic29Topo(), controller=None, switch=OVSSwitch)
    sw = net.get("s1")

    # root namespace node cabled to s1, so the server can reach the manager over tcp
    root = Node("root", inNamespace=False)
    rif = net.addLink(root, sw).intf1
    root.setIP(f"{SDN_IP}/24", intf=rif)
    net.start()

    sw.cmd("ovs-ofctl -O OpenFlow13 add-flow s1 'priority=0,actions=NORMAL'")   # base forwarding rule
    logf = open("sdn_manager.log", "w")
    mgr = subprocess.Popen(["python3", "sdn_manager.py", "--quarantine", str(args.quarantine)],
                           stdout=logf, stderr=subprocess.STDOUT)
    if not wait_port("127.0.0.1", 9999):
        print("sdn manager did not start, check sdn_manager.log")

    h = {n: net.get(f"h{n}") for n in range(1, 9)}
    interval, stagger, crash = SCENARIOS[args.scenario]
    h[1].cmd(f"python3 server.py --scheduler {args.scheduler} > server.log 2>&1 &")
    time.sleep(1)
    for n, (name, speed) in WORKERS.items():
        extra = " --fail-on-task 3 --restart-after 5" if (crash and name == "worker1") else ""
        h[n].cmd(f"python3 worker.py 10.0.0.1 {name} --speed {speed}{extra} > {name}.log 2>&1 &")
    time.sleep(2)      # let all 3 register first
    for i, (n, name) in enumerate(CLIENTS.items()):
        h[n].cmd(f"python3 client.py 10.0.0.1 {name} {TASK_DELAY} --tasks {TASKS_PER_CLIENT} "
                 f"--interval {interval} --start-delay {i * stagger} > {name}.log 2>&1 &")

    print(f"scheduler={args.scheduler} scenario={args.scenario}: {TOTAL_TASKS} tasks from h2-h5, "
          f"workers h6-h8 (speeds 2.0/1.0/0.5)")
    if args.no_cli:
        end = time.time() + 120
        while count_done() < TOTAL_TASKS and time.time() < end:
            time.sleep(1)
        time.sleep(1)
        print(f"completed {count_done()}/{TOTAL_TASKS} tasks")
    else:
        print("wait ~30s, then at the prompt try:  h1 cat server.log | sh cat sdn_manager.log | "
              "sh ovs-ofctl -O OpenFlow13 dump-flows s1 | sh python3 analyze_metrics.py")
        CLI(net)

    for n in h:
        h[n].cmd("kill $(jobs -p) 2>/dev/null")    # stop the background jobs we started
    mgr.terminate()
    logf.close()
    net.stop()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--scheduler", choices=["adaptive", "round_robin"], default="adaptive")
    parser.add_argument("--scenario", choices=list(SCENARIOS), default="failure")
    parser.add_argument("--quarantine", type=int, default=8, help="seconds a failed worker stays blocked")
    parser.add_argument("--no-cli", action="store_true")
    setLogLevel("info")
    run(parser.parse_args())
