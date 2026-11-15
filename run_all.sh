#!/bin/bash
# runs 3 scenarios x 2 schedulers in mininet, about 4 min
# use:  sudo bash run_all.sh      (bash on purpose, shared folders are often mounted noexec)
set -e
mkdir -p results
rm -f results/summary.csv
for scenario in burst light failure; do
  for scheduler in adaptive round_robin; do
    mn -c > /dev/null 2>&1     # clean leftovers from the previous run
    python3 topology.py --scheduler "$scheduler" --scenario "$scenario" --no-cli
    cp metrics.csv "results/${scenario}_${scheduler}_metrics.csv"
    cp sdn_manager.log "results/${scenario}_${scheduler}_sdn.log"
    python3 analyze_metrics.py metrics.csv --scheduler "$scheduler" --scenario "$scenario" --csv results/summary.csv
  done
done
echo; column -s, -t results/summary.csv
