#!/usr/bin/env bash
# Ubuntu 20.04 / 22.04 (Mininet VM works too). Ryu 4.34 is happiest on Python 3.8/3.9.
set -e
sudo apt-get update
sudo apt-get install -y mininet openvswitch-switch iperf python3-pip python3-venv
python3 -m venv --system-site-packages .venv
. .venv/bin/activate
pip install -r requirements.txt
echo "Done. Activate with:  . .venv/bin/activate"
