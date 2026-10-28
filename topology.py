"""One-switch Mininet demonstration for Topic 29."""

import subprocess
import time
from pathlib import Path

from mininet.cli import CLI
from mininet.log import setLogLevel
from mininet.net import Mininet
from mininet.node import OVSSwitch
from mininet.topo import Topo


class Topic29Topo(Topo):
    def build(self):
        switch = self.addSwitch("s1", protocols="OpenFlow13", failMode="standalone")
        for number in range(1, 9):
            host = self.addHost(f"h{number}", ip=f"10.0.0.{number}/24")
            self.addLink(host, switch)


def run():
    network = Mininet(topo=Topic29Topo(), controller=None, switch=OVSSwitch)
    network.start()
    network.get("s1").cmd("ovs-ofctl -O OpenFlow13 add-flow s1 'priority=0,actions=NORMAL'")
    Path("sdn_events.jsonl").write_text("")
    sdn_log = open("sdn_manager.log", "w")
    sdn_manager = subprocess.Popen(["python3", "sdn_manager.py"], stdout=sdn_log, stderr=subprocess.STDOUT)
    h = {number: network.get(f"h{number}") for number in range(1, 9)}

    h[1].cmd("python3 server.py --scheduler adaptive > server.log 2>&1 &")
    time.sleep(1)
    h[6].cmd("python3 worker.py 10.0.0.1 worker1 --fail-after 5 > worker1.log 2>&1 &")
    h[7].cmd("python3 worker.py 10.0.0.1 worker2 > worker2.log 2>&1 &")
    h[8].cmd("python3 worker.py 10.0.0.1 worker3 > worker3.log 2>&1 &")
    time.sleep(2)
    for host, delay in [(2, 6), (3, 2), (4, 1), (5, 3)]:
        h[host].cmd(f"python3 client.py 10.0.0.1 client{host - 1} {delay} > client{host - 1}.log 2>&1 &")

    print("h1 server; h2-h5 clients; h6-h8 workers. worker1 fails during the 6-second task.")
    print("Wait 12 seconds, then use: h1 cat server.log; h1 cat metrics.csv; sh cat sdn_manager.log")
    CLI(network)
    sdn_manager.terminate()
    sdn_log.close()
    network.stop()


if __name__ == "__main__":
    setLogLevel("info")
    run()
