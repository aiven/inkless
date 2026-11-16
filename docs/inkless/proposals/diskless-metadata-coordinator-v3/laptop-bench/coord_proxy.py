#!/usr/bin/env python3
"""Proxy for the coordinator commit path.

Each simulated broker sends one "file" per tick: one record (a commit part) to
every partition of the topic, with acks=all. On a topic with replication factor
3, min.insync.replicas=2, and flush.messages=1, a part's acknowledgment costs
what a coordinator part costs before the SQLite apply: a leader flush, a
follower fetch and flush, and the high-watermark advance. A file completes when
its slowest part is acknowledged, as a produce request that touches every
coordinator partition does.

With --partitions 1 against a diskless topic, the script is a produce probe for
the failover scenario.
"""
import argparse
import json
import multiprocessing
import os
import random
import threading
import time

from confluent_kafka import Producer


def pct(values, q):
    if not values:
        return None
    s = sorted(values)
    return round(s[min(len(s) - 1, int(q * len(s)))], 2)


class Delegate:
    def __init__(self, idx, args, stats):
        self.idx = idx
        self.args = args
        self.stats = stats
        self.producer = Producer({
            "bootstrap.servers": args.bootstrap,
            "acks": "all",
            "linger.ms": 0,
            "enable.idempotence": True,
            "message.timeout.ms": args.timeout_ms,
            "client.id": f"delegate-{idx},diskless_az=az{idx % 3 + 1}",
        })
        self.payload = os.urandom(args.part_bytes)

    def run(self, start, end):
        period = 1.0 / self.args.files_per_sec
        next_tick = start + random.random() * period
        while next_tick < end:
            now = time.time()
            if now < next_tick:
                self.producer.poll(min(next_tick - now, 0.005))
                continue
            # Latency counts from the send, not the tick: VM timer wake-ups lag by up to 10 ms.
            self.send_file(time.time())
            next_tick += period
        self.producer.flush(self.args.timeout_ms / 1000 + 5)

    def send_file(self, t0):
        state = {"left": self.args.partitions, "failed": False, "max": 0.0}
        lock = threading.Lock()
        measured = t0 >= self.stats["measure_from"]

        def on_delivery(err, _msg):
            now = time.time()
            ms = (now - t0) * 1000
            with lock:
                state["left"] -= 1
                if err is not None:
                    state["failed"] = True
                else:
                    state["max"] = max(state["max"], ms)
                    if measured:
                        self.stats["parts"].append(ms)
                done = state["left"] == 0
            if done:
                with self.stats["lock"]:
                    if state["failed"]:
                        self.stats["failed_files"] += 1
                        self.stats["fail_times"].append(now)
                    else:
                        self.stats["ok_times"].append(now)
                        if measured:
                            self.stats["files"].append(state["max"])

        for p in range(self.args.partitions):
            while True:
                try:
                    self.producer.produce(self.args.topic, self.payload, partition=p, on_delivery=on_delivery)
                    break
                except BufferError:
                    self.producer.poll(0.01)
        with self.stats["lock"]:
            self.stats["bytes"] += self.args.partitions * self.args.part_bytes
        self.producer.poll(0)


def run_delegate(idx, args, start, end, q):
    random.seed()
    stats = {"lock": threading.Lock(), "parts": [], "files": [], "ok_times": [], "fail_times": [],
             "failed_files": 0, "bytes": 0, "measure_from": start + args.warmup}
    Delegate(idx, args, stats).run(start, end)
    del stats["lock"], stats["measure_from"]
    q.put(stats)


def max_gap(times, start, end):
    points = [start] + sorted(t for t in times if start <= t <= end) + [end]
    return round(max(b - a for a, b in zip(points, points[1:])), 2)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--bootstrap", default="broker1:9092,broker2:9092,broker3:9092")
    ap.add_argument("--topic", required=True)
    ap.add_argument("--partitions", type=int, required=True, help="N, coordinator partitions")
    ap.add_argument("--brokers", type=int, default=3, help="B, simulated uploading brokers")
    ap.add_argument("--files-per-sec", type=float, default=4.0, help="files per broker per second")
    ap.add_argument("--part-bytes", type=int, default=2400)
    ap.add_argument("--duration", type=float, default=300)
    ap.add_argument("--warmup", type=float, default=60)
    ap.add_argument("--timeout-ms", type=int, default=30000, help="commit deadline")
    ap.add_argument("--label", default="")
    args = ap.parse_args()

    start = time.time() + 2
    end = start + args.warmup + args.duration
    # One process per delegate: delivery callbacks in threads would wait on each other for the GIL.
    ctx = multiprocessing.get_context("fork")
    q = ctx.Queue()
    procs = [ctx.Process(target=run_delegate, args=(i, args, start, end, q)) for i in range(args.brokers)]
    for p in procs:
        p.start()
    stats = {"parts": [], "files": [], "ok_times": [], "fail_times": [], "failed_files": 0, "bytes": 0}
    for _ in procs:
        for k, v in q.get().items():
            stats[k] += v
    for p in procs:
        p.join()

    print(json.dumps({
        "label": args.label,
        "topic": args.topic,
        "N": args.partitions,
        "B": args.brokers,
        "files_per_sec_per_broker": args.files_per_sec,
        "part_bytes": args.part_bytes,
        "files_measured": len(stats["files"]),
        "failed_files": stats["failed_files"],
        "file_ms": {"p50": pct(stats["files"], 0.5), "p99": pct(stats["files"], 0.99), "max": pct(stats["files"], 1.0)},
        "part_ms": {"p50": pct(stats["parts"], 0.5), "p99": pct(stats["parts"], 0.99), "p999": pct(stats["parts"], 0.999)},
        "longest_gap_s": max_gap(stats["ok_times"], start, end),
        "bytes_sent": stats["bytes"],
        "first_failure": min(stats["fail_times"]) if stats["fail_times"] else None,
        "last_failure": max(stats["fail_times"]) if stats["fail_times"] else None,
    }))


if __name__ == "__main__":
    main()
