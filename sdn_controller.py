"""Ryu (OpenFlow 1.3) controller for the Distributed Task System.

Run from the project root:   ryu-manager sdn_controller.py

Functions
  1. L2 learning switch (baseline connectivity).
  2. Application-aware QoS: TCP traffic of the task service (port 5000) is steered into
     OVS queue 1 (guaranteed rate) so tasks/results survive background congestion.
  3. Socket->SDN integration: the scheduler sends UDP events to port 9999
        WORKER_UP / WORKER_DOWN  -> stale-flow cleanup for that host
        BLOCK / UNBLOCK          -> drop rules for unauthorized hosts (all switches)
        LOAD                     -> logged (worker utilisation)
        CONFIG {sdn_aware: bool} -> used by experiments: switch between the static
                                    baseline (plain learning switch) and SDN-aware mode.
  4. Link-down handling: flows through a failed port are removed.
  5. Monitoring: port statistics every 2 s -> results/port_stats.csv, congestion log.
"""
import csv
import json
import os
import socket
import time

from ryu.base import app_manager
from ryu.controller import ofp_event
from ryu.controller.handler import (CONFIG_DISPATCHER, DEAD_DISPATCHER, MAIN_DISPATCHER,
                                    set_ev_cls)
from ryu.lib import hub
from ryu.lib.packet import arp, ether_types, ethernet, ipv4, packet, tcp
from ryu.ofproto import ofproto_v1_3

TASK_PORT = 5000
NOTIFY_PORT = 9999
QUEUE_TASK = 1
PRIO_MISS, PRIO_L2, PRIO_TASK_MISS, PRIO_TASK, PRIO_DROP = 0, 10, 25, 30, 200
LINK_MBPS = float(os.environ.get("LINK_MBPS", "10"))
BLOCK_SECS = int(os.environ.get("BLOCK_SECS", "120"))
STATS_INTERVAL = 2


class TaskSdnController(app_manager.RyuApp):
    OFP_VERSIONS = [ofproto_v1_3.OFP_VERSION]

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.datapaths = {}
        self.mac_to_port = {}
        self.ip_to_mac = {}
        self.blocked = set()
        self.prev = {}
        self.sdn_aware = os.environ.get("SDN_AWARE", "1") == "1"
        os.makedirs("results", exist_ok=True)
        self.events_f = open("results/controller_events.log", "a")
        self.stats_f = open("results/port_stats.csv", "a", newline="")
        self.stats_w = csv.writer(self.stats_f)
        self.stats_w.writerow(["ts", "dpid", "port", "rx_mbps", "tx_mbps", "tx_dropped_delta"])
        hub.spawn(self._notify_listener)
        hub.spawn(self._monitor)
        self._log("START", sdn_aware=self.sdn_aware)

    # ------------------------------------------------------------ helpers
    def _log(self, event, **kw):
        line = "%.3f %s %s" % (time.time(), event, json.dumps(kw))
        self.logger.info(line)
        self.events_f.write(line + "\n")
        self.events_f.flush()

    def _add_flow(self, dp, prio, match, actions, idle=0, hard=0):
        p = dp.ofproto_parser
        inst = [p.OFPInstructionActions(dp.ofproto.OFPIT_APPLY_ACTIONS, actions)] if actions else []
        dp.send_msg(p.OFPFlowMod(datapath=dp, priority=prio, match=match, instructions=inst,
                                 idle_timeout=idle, hard_timeout=hard))

    def _delete(self, dp, match, out_port=None):
        o, p = dp.ofproto, dp.ofproto_parser
        dp.send_msg(p.OFPFlowMod(datapath=dp, command=o.OFPFC_DELETE,
                                 out_port=o.OFPP_ANY if out_port is None else out_port,
                                 out_group=o.OFPG_ANY, match=match))

    def _reset_flows(self, dp):
        """Remove all flows, then install table-miss (+ task-port miss rules in SDN mode)."""
        o, p = dp.ofproto, dp.ofproto_parser
        self._delete(dp, p.OFPMatch())
        to_ctrl = [p.OFPActionOutput(o.OFPP_CONTROLLER, o.OFPCML_NO_BUFFER)]
        self._add_flow(dp, PRIO_MISS, p.OFPMatch(), to_ctrl)
        if self.sdn_aware:
            for fld in ("tcp_dst", "tcp_src"):
                self._add_flow(dp, PRIO_TASK_MISS,
                               p.OFPMatch(eth_type=0x0800, ip_proto=6, **{fld: TASK_PORT}), to_ctrl)
            for ip in self.blocked:
                self._install_block(dp, ip)

    def _install_block(self, dp, ip):
        p = dp.ofproto_parser
        self._add_flow(dp, PRIO_DROP, p.OFPMatch(eth_type=0x0800, ipv4_src=ip), [],
                       hard=BLOCK_SECS)

    # ------------------------------------------------------------ switch lifecycle
    @set_ev_cls(ofp_event.EventOFPSwitchFeatures, CONFIG_DISPATCHER)
    def _features(self, ev):
        dp = ev.msg.datapath
        self.datapaths[dp.id] = dp
        self._reset_flows(dp)
        self._log("SWITCH_UP", dpid=dp.id)

    @set_ev_cls(ofp_event.EventOFPStateChange, [MAIN_DISPATCHER, DEAD_DISPATCHER])
    def _state(self, ev):
        dp = ev.datapath
        if ev.state == MAIN_DISPATCHER:
            self.datapaths[dp.id] = dp
        elif dp.id in self.datapaths:
            del self.datapaths[dp.id]

    @set_ev_cls(ofp_event.EventOFPPortStatus, MAIN_DISPATCHER)
    def _port_status(self, ev):
        msg, dp = ev.msg, ev.msg.datapath
        o, p = dp.ofproto, dp.ofproto_parser
        desc = msg.desc
        down = (bool(desc.state & o.OFPPS_LINK_DOWN) or bool(desc.config & o.OFPPC_PORT_DOWN)
                or msg.reason == o.OFPPR_DELETE)
        if down and self.sdn_aware:
            self._log("LINK_DOWN", dpid=dp.id, port=desc.port_no)
            self._delete(dp, p.OFPMatch(), out_port=desc.port_no)
            self._delete(dp, p.OFPMatch(in_port=desc.port_no))
            tbl = self.mac_to_port.get(dp.id, {})
            for mac in [m for m, pt in tbl.items() if pt == desc.port_no]:
                del tbl[mac]

    # ------------------------------------------------------------ packet-in
    @set_ev_cls(ofp_event.EventOFPPacketIn, MAIN_DISPATCHER)
    def _packet_in(self, ev):
        msg, dp = ev.msg, ev.msg.datapath
        o, p = dp.ofproto, dp.ofproto_parser
        in_port = msg.match["in_port"]
        pkt = packet.Packet(msg.data)
        eth = pkt.get_protocol(ethernet.ethernet)
        if eth is None or eth.ethertype == ether_types.ETH_TYPE_LLDP:
            return
        ip4, t, a = pkt.get_protocol(ipv4.ipv4), pkt.get_protocol(tcp.tcp), pkt.get_protocol(arp.arp)
        if ip4:
            self.ip_to_mac[ip4.src] = eth.src
        if a:
            self.ip_to_mac[a.src_ip] = eth.src
        if self.sdn_aware and ip4 and ip4.src in self.blocked:
            return
        tbl = self.mac_to_port.setdefault(dp.id, {})
        tbl[eth.src] = in_port
        out_port = tbl.get(eth.dst, o.OFPP_FLOOD)
        actions = [p.OFPActionOutput(out_port)]
        if out_port != o.OFPP_FLOOD:
            if self.sdn_aware and ip4 and t and TASK_PORT in (t.src_port, t.dst_port):
                port_fld = {"tcp_dst": TASK_PORT} if t.dst_port == TASK_PORT else {"tcp_src": TASK_PORT}
                match = p.OFPMatch(eth_type=0x0800, ip_proto=6, ipv4_src=ip4.src,
                                   ipv4_dst=ip4.dst, **port_fld)
                actions = [p.OFPActionSetQueue(QUEUE_TASK), p.OFPActionOutput(out_port)]
                self._add_flow(dp, PRIO_TASK, match, actions, idle=60)
            else:
                match = p.OFPMatch(in_port=in_port, eth_src=eth.src, eth_dst=eth.dst)
                self._add_flow(dp, PRIO_L2, match, actions, idle=30)
        data = msg.data if msg.buffer_id == o.OFP_NO_BUFFER else None
        dp.send_msg(p.OFPPacketOut(datapath=dp, buffer_id=msg.buffer_id, in_port=in_port,
                                   actions=actions, data=data))

    # ------------------------------------------------------------ northbound (socket app -> SDN)
    def _notify_listener(self):
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        s.bind(("0.0.0.0", NOTIFY_PORT))
        while True:
            data, addr = s.recvfrom(65535)
            try:
                self._handle_event(json.loads(data.decode()))
            except Exception as e:  # noqa: BLE001 - never kill the listener
                self.logger.warning("bad notification from %s: %s", addr, e)

    def _handle_event(self, ev):
        kind, ip = ev.get("event"), ev.get("ip")
        if kind == "CONFIG":
            self.sdn_aware = bool(ev.get("sdn_aware", True))
            self.blocked.clear()
            for dp in list(self.datapaths.values()):
                self._reset_flows(dp)
            self._log("CONFIG", sdn_aware=self.sdn_aware)
        elif kind == "LOAD":
            self._log("LOAD", pending=ev.get("pending"), workers=ev.get("workers"))
        elif kind == "WORKER_UP":
            self._log("WORKER_UP", worker=ev.get("worker_id"), ip=ip)
        elif kind == "WORKER_DOWN":
            self._log("WORKER_DOWN", worker=ev.get("worker_id"), ip=ip, reason=ev.get("reason"),
                      requeued=ev.get("requeued"))
            if self.sdn_aware:
                self._flush_host(ip)
        elif kind == "BLOCK" and ip:
            self._log("BLOCK", ip=ip, reason=ev.get("reason"))
            if self.sdn_aware:
                self.blocked.add(ip)
                for dp in list(self.datapaths.values()):
                    self._install_block(dp, ip)
        elif kind == "UNBLOCK" and ip:
            self.blocked.discard(ip)
            for dp in list(self.datapaths.values()):
                self._delete(dp, dp.ofproto_parser.OFPMatch(eth_type=0x0800, ipv4_src=ip))
            self._log("UNBLOCK", ip=ip)

    def _flush_host(self, ip):
        """Remove learned state/flows of a failed worker so no stale rule black-holes traffic."""
        mac = self.ip_to_mac.get(ip)
        for dp in list(self.datapaths.values()):
            p = dp.ofproto_parser
            for fld in ("ipv4_dst", "ipv4_src"):
                self._delete(dp, p.OFPMatch(eth_type=0x0800, **{fld: ip}))
            if mac:
                self._delete(dp, p.OFPMatch(eth_dst=mac))
                self._delete(dp, p.OFPMatch(eth_src=mac))
                self.mac_to_port.get(dp.id, {}).pop(mac, None)
        self._log("FLUSH_HOST", ip=ip, mac=mac)

    # ------------------------------------------------------------ monitoring
    def _monitor(self):
        while True:
            hub.sleep(STATS_INTERVAL)
            for dp in list(self.datapaths.values()):
                dp.send_msg(dp.ofproto_parser.OFPPortStatsRequest(dp, 0, dp.ofproto.OFPP_ANY))

    @set_ev_cls(ofp_event.EventOFPPortStatsReply, MAIN_DISPATCHER)
    def _stats_reply(self, ev):
        now, dpid = time.time(), ev.msg.datapath.id
        for st in sorted(ev.msg.body, key=lambda s: s.port_no):
            if st.port_no >= 0xffffff00:
                continue
            key, cur = (dpid, st.port_no), (now, st.rx_bytes, st.tx_bytes, st.tx_dropped)
            prev, self.prev[key] = self.prev.get(key), cur
            if not prev or now - prev[0] <= 0:
                continue
            dt = now - prev[0]
            rx, tx = (cur[1] - prev[1]) * 8 / dt / 1e6, (cur[2] - prev[2]) * 8 / dt / 1e6
            self.stats_w.writerow(["%.3f" % now, dpid, st.port_no, "%.3f" % rx, "%.3f" % tx,
                                   cur[3] - prev[3]])
            self.stats_f.flush()
            if tx >= 0.8 * LINK_MBPS:
                self._log("CONGESTION", dpid=dpid, port=st.port_no, tx_mbps=round(tx, 2))
