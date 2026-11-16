#!/usr/bin/env python3
"""Reads broker metrics from the JMX exporters, and computes inter-AZ bytes.

  prom.py snap                      prints the selected metrics of every broker as JSON
  prom.py interaz BEFORE AFTER ...  prints inter-AZ bytes per produced MB for both backends
"""
import argparse
import json
import re
import urllib.request

BROKERS = ["broker1", "broker2", "broker3"]
WANTED = [
    ("produce_total_ms", re.compile(r'^kafka_network_requestmetrics_totaltimems\{.*request="Produce"')),
    ("file_commit_ms", re.compile(r"^kafka_inkless_.*filecommittime\{")),
    ("find_batches_ms", re.compile(r"^kafka_inkless_.*findbatchestime\{")),
    ("log_flush_ms", re.compile(r"^kafka_log_logflushstats_logflushrateandtimems\{")),
    ("replication_bytes_out", re.compile(r"^kafka_server_brokertopicmetrics_replicationbytesoutpersec(_total)?\s")),
]
QUANTILE = re.compile(r'quantile="([0-9.]+)"')


def snap():
    out = {}
    for b in BROKERS:
        metrics = {}
        try:
            text = urllib.request.urlopen(f"http://{b}:7070/metrics", timeout=10).read().decode()
        except OSError as e:
            out[b] = {"error": str(e)}
            continue
        for line in text.splitlines():
            for key, rx in WANTED:
                if rx.search(line):
                    q = QUANTILE.search(line)
                    if key.endswith("_ms") and not q:
                        continue
                    name = f"{key}_p{q.group(1)[2:]}" if q else key
                    metrics[name] = float(line.rsplit(" ", 1)[1])
        out[b] = metrics
    print(json.dumps(out))


def interaz(args):
    before, after = (json.load(open(f)) for f in (args.before, args.after))
    repl = sum(after[b].get("replication_bytes_out", 0) - before[b].get("replication_bytes_out", 0) for b in BROKERS)
    produced_mb = args.produced_bytes / 2**20
    pg_total = args.pg_primary_bytes - args.pg_standby_bytes
    # Two thirds of brokers sit outside the primary's AZ; the standby sits in another AZ.
    pg_cross = 2 / 3 * pg_total + args.pg_standby_bytes
    # Two thirds of parts cross to a leader in another AZ; follower copies always cross with rack spread.
    coord_cross = 2 / 3 * args.proxy_bytes + repl
    print(json.dumps({
        "produced_mb": round(produced_mb, 1),
        "postgres_interaz_bytes_per_mb": round(pg_cross / produced_mb) if args.pg_primary_bytes else None,
        "coordinator_interaz_bytes_per_mb": round(coord_cross / produced_mb) if args.proxy_bytes else None,
        "replication_bytes": repl,
    }))


def main():
    ap = argparse.ArgumentParser()
    sub = ap.add_subparsers(dest="cmd", required=True)
    sub.add_parser("snap")
    ia = sub.add_parser("interaz")
    ia.add_argument("before")
    ia.add_argument("after")
    ia.add_argument("--produced-bytes", type=float, required=True)
    ia.add_argument("--proxy-bytes", type=float, default=0)
    ia.add_argument("--pg-primary-bytes", type=float, default=0, help="rx+tx delta on pg-primary")
    ia.add_argument("--pg-standby-bytes", type=float, default=0, help="rx+tx delta on pg-standby")
    args = ap.parse_args()
    snap() if args.cmd == "snap" else interaz(args)


if __name__ == "__main__":
    main()
