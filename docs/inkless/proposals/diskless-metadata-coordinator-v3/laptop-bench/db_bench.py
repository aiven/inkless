#!/usr/bin/env python3
"""SQLite (one coordinator partition) against Postgres (the whole cluster).

Both engines hold the same simplified batches table. Postgres holds every row
and takes every commit and read. One SQLite database holds 1/N of the rows and
takes 1/N of the commits and reads, as one coordinator partition does. At each
ladder step, the script measures apply latency and paced findBatches latency
while both run together, and the SQLite backup duration.

The schema and queries are stand-ins for Inkless's commit_file and
find_batches_v2, so the result compares engines and placement, not the real
functions.
"""
import argparse
import json
import multiprocessing
import os
import random
import sqlite3
import threading
import time

import psycopg

SCHEMA = """
CREATE TABLE IF NOT EXISTS logs (log_id INTEGER PRIMARY KEY, high_watermark BIGINT NOT NULL);
CREATE TABLE IF NOT EXISTS batches (
  batch_id BIGINT PRIMARY KEY,
  log_id INTEGER NOT NULL,
  base_offset BIGINT NOT NULL,
  last_offset BIGINT NOT NULL,
  file_id BIGINT NOT NULL,
  byte_offset BIGINT NOT NULL,
  byte_size INTEGER NOT NULL,
  max_timestamp BIGINT NOT NULL
);
CREATE INDEX IF NOT EXISTS batches_by_log ON batches (log_id, last_offset);
"""
FIND = ("SELECT batch_id, base_offset, last_offset, file_id, byte_offset, byte_size FROM batches "
        "WHERE log_id = {p} AND last_offset >= {p} ORDER BY last_offset LIMIT 50")
RECORDS_PER_BATCH = 10


def pct(values, q):
    if not values:
        return None
    s = sorted(values)
    return round(s[min(len(s) - 1, int(q * len(s)))], 2)


class Engine:
    def __init__(self, name, logs, connect, ph):
        self.name, self.logs, self.connect, self.ph = name, logs, connect, ph
        self.next_id = 0
        self.lock = threading.Lock()

    def setup(self):
        with self.connect() as c:
            cur = c.cursor()
            for stmt in filter(str.strip, SCHEMA.split(";")):
                cur.execute(stmt)
            cur.execute("SELECT COALESCE(MAX(batch_id), -1) + 1 FROM batches")
            self.next_id = cur.fetchone()[0]
            c.commit()

    def load_to(self, rows):
        """Bulk-loads rows round-robin over the logs, offsets ascending per log."""
        if rows <= self.next_id:
            return
        lo, hi, n = self.next_id, rows - 1, len(self.logs)
        with self.connect() as c:
            cur = c.cursor()
            mod = "%%" if self.name == "postgres" else "%"
            select = (f"SELECT x, x {mod} {n}, (x / {n}) * {RECORDS_PER_BATCH}, (x / {n}) * {RECORDS_PER_BATCH} + "
                      f"{RECORDS_PER_BATCH - 1}, x / 200, (x {mod} 200) * 8192, 8192, x FROM ")
            if self.name == "postgres":
                cur.execute(f"INSERT INTO batches {select} generate_series(%s::bigint, %s::bigint) AS x", (lo, hi))
            else:
                cur.execute(f"WITH RECURSIVE s(x) AS (SELECT ? UNION ALL SELECT x + 1 FROM s WHERE x < ?) "
                            f"INSERT INTO batches {select} s", (lo, hi))
            hw = (rows // n + 1) * RECORDS_PER_BATCH
            for i, log in enumerate(self.logs):
                cur.execute(f"DELETE FROM logs WHERE log_id = {self.ph}", (i,))
                cur.execute(f"INSERT INTO logs VALUES ({self.ph}, {self.ph})", (i, hw))
            c.commit()
        self.next_id = rows

    def apply(self, conn, rows_per_commit):
        """One commit: advances the high watermark of each partition in the file and inserts its batch rows."""
        cur = conn.cursor()
        with self.lock:
            first = self.next_id
            self.next_id += rows_per_commit
        logs = sorted({(first + k) % len(self.logs) for k in range(rows_per_commit)})
        cur.execute(f"UPDATE logs SET high_watermark = high_watermark + {RECORDS_PER_BATCH} "
                    f"WHERE log_id IN ({','.join([self.ph] * len(logs))}) RETURNING log_id, high_watermark", logs)
        values = []
        for k, (log, hw) in enumerate(cur.fetchall()):
            values += [first + k, log, hw - RECORDS_PER_BATCH, hw - 1, first // 200, k * 8192, 8192, first]
        row = f"({','.join([self.ph] * 8)})"
        cur.execute(f"INSERT INTO batches VALUES {','.join([row] * (len(values) // 8))}", values)
        conn.commit()

    def find(self, conn, partitions, max_offset):
        cur = conn.cursor()
        q = FIND.format(p=self.ph)
        for _ in range(partitions):
            cur.execute(q, (random.randrange(len(self.logs)), random.randrange(max_offset)))
            cur.fetchall()
        conn.commit()


def paced(rate, end, fn, out):
    """Calls fn at a fixed rate and records each call's latency."""
    # Latency counts from the call, not the schedule: VM timer wake-ups lag by up to 10 ms.
    period, t = 1.0 / rate, time.time()
    while t < end:
        now = time.time()
        if now < t:
            time.sleep(t - now)
        start = time.time()
        fn()
        out.append((time.time() - start) * 1000)
        t += period


def checkpointer(path, stop):
    conn = sqlite3.connect(path, check_same_thread=False)
    page = conn.execute("PRAGMA page_size").fetchone()[0]
    while not stop.is_set():
        try:
            if os.path.getsize(path + "-wal") / page > 1000:
                conn.execute("PRAGMA wal_checkpoint(PASSIVE)")
        except FileNotFoundError:
            pass
        stop.wait(0.1)
    conn.close()


def reader(engine, rate, end, partitions, max_offset, q):
    conn = engine.connect()
    out = []
    paced(rate, end, lambda: engine.find(conn, partitions, max_offset), out)
    q.put(out)
    q.close()
    q.join_thread()
    # Skips interpreter cleanup, which would close the parent's inherited Postgres sockets.
    os._exit(0)


def measure(engine, args, rows, commit_rate, rows_per_commit, fetch_rate):
    applies, finds = [], []
    end = time.time() + args.measure
    max_offset = (rows // len(engine.logs)) * RECORDS_PER_BATCH
    # Readers run in processes: Python threads contend on the GIL while building result rows.
    ctx = multiprocessing.get_context("fork")
    q = ctx.Queue()
    readers = [ctx.Process(target=reader, args=(engine, fetch_rate / args.readers, end, args.partitions_per_fetch,
                                                max_offset, q)) for _ in range(args.readers)]
    for p in readers:
        p.start()
    wconn = engine.connect()
    paced(commit_rate, end, lambda: engine.apply(wconn, rows_per_commit), applies)
    wconn.close()
    for _ in readers:
        finds += q.get()
    for p in readers:
        p.join()
    return {"apply_ms": {"p50": pct(applies, 0.5), "p99": pct(applies, 0.99)},
            "find_ms": {"p50": pct(finds, 0.5), "p99": pct(finds, 0.99)},
            "applies": len(applies), "finds": len(finds)}


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--ladder", default="1000000,10000000", help="cluster-wide batch rows per step")
    ap.add_argument("--n", type=int, default=8, help="coordinator partitions")
    ap.add_argument("--b", type=int, default=3, help="brokers")
    ap.add_argument("--logs", type=int, default=192, help="diskless partitions in the cluster")
    ap.add_argument("--rows-per-file", type=int, default=192, help="batch rows per file")
    ap.add_argument("--fetch-rate", type=float, default=200, help="cluster-wide findBatches calls per second")
    ap.add_argument("--partitions-per-fetch", type=int, default=8)
    ap.add_argument("--readers", type=int, default=4, help="read threads, as diskless.metadata.read.threads")
    ap.add_argument("--measure", type=float, default=60)
    ap.add_argument("--sqlite", default="/sqlite/coord.db")
    ap.add_argument("--pg", default="host=pg-primary dbname=bench user=admin password=admin")
    args = ap.parse_args()

    def sqlite_connect():
        c = sqlite3.connect(args.sqlite, check_same_thread=False, isolation_level="DEFERRED")
        c.execute("PRAGMA journal_mode=WAL")
        c.execute("PRAGMA synchronous=NORMAL")
        c.execute("PRAGMA wal_autocheckpoint=0")
        return c

    files_per_sec = args.b * 4
    pg = Engine("postgres", list(range(args.logs)), lambda: psycopg.connect(args.pg), "%s")
    lite = Engine("sqlite", list(range(args.logs // args.n)), sqlite_connect, "?")
    for e in (pg, lite):
        e.setup()

    stop = threading.Event()
    threading.Thread(target=checkpointer, args=(args.sqlite, stop), daemon=True).start()
    try:
        for rows in (int(r) for r in args.ladder.split(",")):
            t0 = time.time()
            pg.load_to(rows)
            lite.load_to(rows // args.n)
            load_s = round(time.time() - t0, 1)

            result = {"rows_cluster": rows, "load_s": load_s}
            result["postgres"] = measure(pg, args, rows, files_per_sec, args.rows_per_file, args.fetch_rate)
            result["sqlite"] = measure(lite, args, rows // args.n, files_per_sec,
                                       max(1, args.rows_per_file // args.n), args.fetch_rate / args.n)

            src = sqlite_connect()
            dst = sqlite3.connect(args.sqlite + ".backup")
            t0 = time.time()
            src.backup(dst)
            result["sqlite"]["backup_s"] = round(time.time() - t0, 2)
            result["sqlite"]["db_mib"] = round(os.path.getsize(args.sqlite) / 2**20, 1)
            dst.close()
            src.close()
            os.remove(args.sqlite + ".backup")
            with psycopg.connect(args.pg) as c:
                result["postgres"]["db_mib"] = round(
                    c.execute("SELECT pg_database_size(current_database())").fetchone()[0] / 2**20, 1)
            print(json.dumps(result), flush=True)
    finally:
        stop.set()


if __name__ == "__main__":
    main()
