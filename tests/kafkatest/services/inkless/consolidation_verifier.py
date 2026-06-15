# Inkless
# Copyright (C) 2024 - 2026 Aiven OY
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU Affero General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU Affero General Public License for more details.
#
# You should have received a copy of the GNU Affero General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.

"""Helper to assert the inkless consolidation pipeline moved and pruned data.

Exposes three signals about a consolidating topic: the JMX consolidation gauges,
MinIO object counts (via ``mc``), and the Postgres control plane (via ``psql``).
Construct it with a started ``KafkaService`` and call the helpers from a test;
the Postgres/MinIO deps and the mc/psql clients are provided by the system-test
harness (see ``.github/workflows/inkless-system-tests.yml`` and the Dockerfile).
"""

import csv
import json
import shlex
import time


class ConsolidationVerifier(object):
    # Broker-aggregate consolidation gauges; Yammer gauges expose "Value".
    JMX_PACKAGE = "io.aiven.inkless.consolidation"
    JMX_TYPE = "ConsolidationMetrics"
    TOTAL_LAG = "ConsolidationTotalLag"
    LOCAL_LAG = "ConsolidationLocalLag"
    DELETABLE_MESSAGES = "ConsolidationDeletableMessages"
    JMX_ATTRIBUTE = "Value"

    # MinIO (on ducknet alias "storage").
    MINIO_ALIAS = "systest"
    MINIO_ENDPOINT = "http://storage:9000"
    MINIO_BUCKET = "inkless"
    MINIO_ACCESS_KEY = "minioadmin"
    MINIO_SECRET_KEY = "minioadmin"
    # rsm.config.key.prefix; WAL objects live at the bucket root instead.
    TIERED_PREFIX = "tiered-storage/"

    # Postgres control plane (on ducknet alias "postgres").
    PG_HOST = "postgres"
    PG_PORT = 5432
    PG_DB = "inkless"
    PG_USER = "admin"
    PG_PASSWORD = "admin"

    def __init__(self, kafka):
        self.kafka = kafka
        self.logger = kafka.logger
        self._mc_alias_ready = False

    # --------------------- JMX ---------------------

    @classmethod
    def aggregate_object_name(cls, name):
        # Object name for the broker-aggregate gauge (property order is irrelevant).
        return "%s:type=%s,name=%s" % (cls.JMX_PACKAGE, cls.JMX_TYPE, name)

    @classmethod
    def aggregate_object_names(cls):
        # Object names for the three broker-aggregate gauges.
        return [cls.aggregate_object_name(n)
                for n in (cls.TOTAL_LAG, cls.LOCAL_LAG, cls.DELETABLE_MESSAGES)]

    @classmethod
    def find_aggregate_key(cls, keys, name):
        # Locate the aggregate gauge's CSV key, ignoring key-property ordering and
        # skipping per-partition keys. Returns the key, or None if absent.
        attr_suffix = ":" + cls.JMX_ATTRIBUTE
        for k in keys:
            if not (k.startswith(cls.JMX_PACKAGE + ":") and k.endswith(attr_suffix)):
                continue
            if ("type=%s" % cls.JMX_TYPE) not in k:
                continue
            if "topic=" in k or "partition=" in k:
                continue
            if ("name=%s," % name) in k or ("name=%s:" % name) in k:
                return k
        return None

    def start_jmx(self):
        # Start JmxTool on the broker nodes only (the controller never exposes
        # these MBeans, so wiring it into KafkaService would hang --wait).
        self.kafka.jmx_object_names = self.aggregate_object_names()
        self.kafka.jmx_attributes = [self.JMX_ATTRIBUTE]
        for node in self.kafka.nodes:
            self.kafka.start_jmx_tool(self.kafka.idx(node), node)

    def latest_jmx_values(self):
        # Most recent sample of each monitored attribute, summed across brokers
        # (only the partition leader reports non-zero, so summing is safe).
        totals = {}
        for node in self.kafka.nodes:
            sample = self._latest_jmx_sample_on(node)
            for key, value in sample.items():
                totals[key] = totals.get(key, 0.0) + value
        return totals

    def _latest_jmx_sample_on(self, node):
        log = self.kafka.jmx_tool_log
        try:
            header = list(node.account.ssh_capture("head -n 1 %s" % log, allow_fail=True))
            last = list(node.account.ssh_capture("tail -n 1 %s" % log, allow_fail=True))
        except Exception as e:  # noqa: BLE001 - best effort; missing log -> no sample
            self.logger.debug("Could not read JMX log on %s: %s" % (node.account, e))
            return {}
        if not header or not last:
            return {}
        # Parse with csv: JMX ObjectNames contain commas (property separators) and
        # are quoted in the header, so a naive comma split would misalign column
        # names with their values. Header: "time","<objectName>:<attr>",...
        names = next(csv.reader([header[0].strip()]), [])
        data_line = last[0].strip()
        if not data_line or data_line.startswith('"'):
            # Only the header has been written so far; no sample yet.
            return {}
        fields = next(csv.reader([data_line]), [])
        sample = {}
        for i in range(1, len(names)):
            if i >= len(fields):
                break
            try:
                sample[names[i]] = float(fields[i])
            except ValueError:
                continue
        return sample

    def _aggregate_value(self, name):
        values = self.latest_jmx_values()
        key = self.find_aggregate_key(values.keys(), name)
        return values.get(key, 0.0) if key is not None else 0.0

    def total_lag(self):
        return self._aggregate_value(self.TOTAL_LAG)

    def local_lag(self):
        return self._aggregate_value(self.LOCAL_LAG)

    def deletable_messages(self):
        return self._aggregate_value(self.DELETABLE_MESSAGES)

    # --------------------- Object store ---------------------

    def _storage_node(self):
        # Any broker node resolves the postgres/storage aliases on ducknet.
        return self.kafka.nodes[0]

    def _ensure_mc_alias(self, node):
        if self._mc_alias_ready:
            return
        cmd = "mc alias set %s %s %s %s" % (
            self.MINIO_ALIAS, self.MINIO_ENDPOINT,
            self.MINIO_ACCESS_KEY, self.MINIO_SECRET_KEY)
        rc = None
        for _ in range(5):
            rc = node.account.ssh(cmd, allow_fail=True)
            if rc == 0:
                self._mc_alias_ready = True
                return
            time.sleep(2)
        raise AssertionError(
            "Failed to configure mc alias '%s' against %s (rc=%s). Check MinIO "
            "connectivity/credentials for the inkless system-test dependencies."
            % (self.MINIO_ALIAS, self.MINIO_ENDPOINT, rc))

    def object_keys(self, prefix=""):
        # Object keys under the given bucket prefix, via mc ls --recursive --json.
        node = self._storage_node()
        self._ensure_mc_alias(node)
        target = "%s/%s" % (self.MINIO_ALIAS, self.MINIO_BUCKET)
        if prefix:
            target += "/" + prefix
        cmd = "mc ls --recursive --json %s" % target
        keys = []
        for line in node.account.ssh_capture(cmd, allow_fail=False):
            line = line.strip()
            if not line:
                continue
            try:
                obj = json.loads(line)
            except ValueError:
                raise AssertionError(
                    "Unexpected non-JSON output from '%s': %s" % (cmd, line))
            status = obj.get("status")
            if status == "error":
                err = obj.get("error") or {}
                detail = err.get("message") or err.get("cause") or obj
                raise AssertionError("mc failed listing %s: %s" % (target, detail))
            if status != "success":
                continue
            key = obj.get("key")
            if key:
                keys.append(key)
        return keys

    def tiered_object_count(self):
        # List only the tiered-storage prefix: the bucket persists across runs, so
        # scanning the whole bucket here (called repeatedly in wait_until) would
        # get slower and flakier over time.
        return len(self.object_keys(self.TIERED_PREFIX))

    def wal_object_count(self):
        # Objects outside the tiered-storage prefix, i.e. diskless WAL files.
        return len([k for k in self.object_keys() if not k.startswith(self.TIERED_PREFIX)])

    # --------------------- Control plane ---------------------

    def _psql(self, sql):
        node = self._storage_node()
        cmd = "PGPASSWORD=%s psql -h %s -p %d -U %s -d %s -tAc %s" % (
            shlex.quote(self.PG_PASSWORD), shlex.quote(self.PG_HOST), self.PG_PORT,
            shlex.quote(self.PG_USER), shlex.quote(self.PG_DB), shlex.quote(sql))
        return "".join(node.account.ssh_capture(cmd, allow_fail=False)).strip()

    @staticmethod
    def _sql_literal(value):
        # SQL string literal with single quotes doubled (standard escaping), so a
        # topic with a quote can't break or inject into the query.
        return "'%s'" % str(value).replace("'", "''")

    def _psql_int(self, sql, default=0):
        out = self._psql(sql)
        if out == "" or out is None:
            return default
        try:
            return int(out.splitlines()[0].strip())
        except (ValueError, IndexError):
            return default

    def wal_batch_count(self, topic):
        # WAL batch rows for the topic. Returns -1 when the topic has no `logs` rows
        # yet, so "not registered" stays distinct from "registered and pruned to 0".
        literal = self._sql_literal(topic)
        return self._psql_int(
            "SELECT CASE WHEN EXISTS (SELECT 1 FROM logs WHERE topic_name = %s) "
            "THEN (SELECT count(*) FROM batches b JOIN logs l ON b.topic_id = l.topic_id "
            "WHERE l.topic_name = %s) ELSE -1 END" % (literal, literal),
            default=-1)

    def min_log_start_offset(self, topic):
        # Min log_start_offset across partitions; advances past 0 once WAL is pruned.
        return self._psql_int(
            "SELECT coalesce(min(log_start_offset), 0) FROM logs WHERE topic_name = %s"
            % self._sql_literal(topic))

    # --------------------- Tooling ---------------------

    def verify_tooling(self):
        # Fail fast if the ducker image lacks the mc/psql clients.
        node = self._storage_node()
        missing = []
        for tool in ("mc", "psql"):
            if node.account.ssh("command -v %s" % tool, allow_fail=True) != 0:
                missing.append(tool)
        assert not missing, (
            "Missing client tool(s) %s on the broker node. Rebuild the ducker image "
            "(tests/docker/Dockerfile installs `mc` and `postgresql-client`) with "
            "`tests/docker/ducker-ak up` before running consolidation pipeline tests." % missing)
