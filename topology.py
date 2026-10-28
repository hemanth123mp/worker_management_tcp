#!/usr/bin/env python3
"""Mininet topology for the Distributed Task System.

                 root-ns (controller notifications, 10.0.0.254)
                        |
   sched(.1) client(.2) bgc(.20) -- s1 -- s2 -- w1(.11) w2(.12) bgs(.21)
                                      \
                                       s3 -- w3(.13) w4(.14) attacker(.66)

All switch ports are rate limited to 10 Mbit/s using OVS linux-htb QoS with two queues
(q0 best effort, q1 guaranteed for task traffic).  Controller: remote Ryu on 127.0.0.1:6653.
Run interactively:  sudo python3 topology.py
"""
from mininet.cli import CLI
from mininet.link import Link
from mininet.log import info, setLogLevel
from mininet.net import Mininet
from mininet.node import Node, OVSKernelSwitch, RemoteController

NOTIFY_IP = "10.0.0.254"
HOST_IPS = {"sched": "10.0.0.1", "client": "10.0.0.2", "bgc": "10.0.0.20", "bgs": "10.0.0.21",
            "w1": "10.0.0.11", "w2": "10.0.0.12", "w3": "10.0.0.13", "w4": "10.0.0.14",
            "att": "10.0.0.66"}
LAYOUT = {"s1": ["sched", "client", "bgc"], "s2": ["w1", "w2", "bgs"], "s3": ["w3", "w4", "att"]}
LINK_BPS = 10_000_000


def build(ctrl_ip="127.0.0.1", ctrl_port=6653):
    net = Mininet(controller=None, switch=OVSKernelSwitch, link=Link, autoSetMacs=True, build=False)
    net.addController("c0", controller=RemoteController, ip=ctrl_ip, port=ctrl_port)
    sw = {n: net.addSwitch(n, protocols="OpenFlow13") for n in LAYOUT}
    for s, hosts in LAYOUT.items():
        for h in hosts:
            net.addLink(net.addHost(h, ip=HOST_IPS[h] + "/24"), sw[s])
    net.addLink(sw["s1"], sw["s2"])
    net.addLink(sw["s1"], sw["s3"])
    root = Node("root", inNamespace=False)           # root namespace = where Ryu runs
    intf = net.addLink(root, sw["s1"]).intf1
    root.setIP(NOTIFY_IP, prefixLen=24, intf=intf)
    return net


def configure_qos(net, bps=LINK_BPS):
    """q0: best effort (min 20%), q1: task traffic (min 70%); both may burst to line rate."""
    for s in net.switches:
        for intf in s.intfList():
            if intf.name == "lo":
                continue
            s.cmd("ovs-vsctl -- set port %s qos=@q -- --id=@q create qos type=linux-htb "
                  "other-config:max-rate=%d queues:0=@q0 queues:1=@q1 "
                  "-- --id=@q0 create queue other-config:min-rate=%d other-config:max-rate=%d "
                  "-- --id=@q1 create queue other-config:min-rate=%d other-config:max-rate=%d"
                  % (intf.name, bps, bps * 0.2, bps, bps * 0.7, bps))


def start(ctrl_ip="127.0.0.1", ctrl_port=6653):
    net = build(ctrl_ip, ctrl_port)
    net.start()
    net.waitConnected()
    configure_qos(net)
    return net


if __name__ == "__main__":
    setLogLevel("info")
    n = start()
    info("*** Pre-check connectivity\n")
    n.pingAll()
    info("\nManual run example (in the Mininet CLI):\n"
         "  sched python3 scheduler.py --ctrl-host 10.0.0.254 &\n"
         "  w1 python3 worker.py --id w1 --server 10.0.0.1 --speed 2.0 &\n"
         "  client python3 client.py --server 10.0.0.1 --tasks 100 --out results/manual.json\n")
    CLI(n)
    n.stop()
