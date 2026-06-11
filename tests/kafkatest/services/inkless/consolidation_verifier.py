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

"""Helpers to assert that the inkless consolidation pipeline actually moved and
pruned data, rather than just that produce/consume still works.

The verifier exposes three independent signals about a consolidating topic:

  1. JMX   - the ``io.aiven.inkless.consolidation`` broker-aggregate gauges
             (``ConsolidationTotalLag``, ``ConsolidationLocalLag``,
             ``ConsolidationDeletableMessages``). These show lag rising while a
             produce is in flight and draining to ~0 once the diskless WAL has
             been copied to the local log and tiered to remote storage.
  2. Object store - object counts under the diskless WAL keys (bucket root) vs
             the tiered-storage prefix in MinIO, queried with the ``mc`` client.
             This proves data physically *moved* to remote, and that WAL objects
             are removed by the file cleaner after a prune.
  3. Control plane - WAL ``batches`` rows and ``logs.log_start_offset`` per
             topic-partition in the Postgres control plane, queried with
             ``psql``. This is the authoritative signal that the WAL was pruned.

The external Postgres and MinIO containers are started by
``tests/docker/run_consolidation_tests.sh`` and attached to ``ducknet`` with the
network aliases ``postgres`` and ``storage``; the broker nodes (which we shell
into) are on the same network and resolve those names. The ``mc`` and ``psql``
clients are installed in the ducker image (see ``tests/docker/Dockerfile``).

This is a plain helper rather than a ducktape Service: construct it with a
started ``KafkaService`` and call the query/wait helpers from a test.
"""

import json


class ConsolidationVerifier(object):
    # --- JMX -----------------------------------------------------------------
    # KafkaMetricsGroup("io.aiven.inkless.consolidation", "ConsolidationMetrics")
    # registers broker-level aggregate gauges (sum across the partitions this
    # broker leads) with no topic/partition tags, plus per-partition gauges
    # tagged with topic + partition. Yammer gauges expose the "Value" attribute.
    JMX_PACKAGE = "io.aiven.inkless.consolidation"
    JMX_TYPE = "ConsolidationMetrics"
    TOTAL_LAG = "ConsolidationTotalLag"
    LOCAL_LAG = "ConsolidationLocalLag"
    DELETABLE_MESSAGES = "ConsolidationDeletableMessages"
    JMX_ATTRIBUTE = "Value"

    # --- External dependencies (aliases on ducknet) --------------------------
    MINIO_ALIAS = "systest"
    MINIO_ENDPOINT = "http://storage:9000"
    MINIO_BUCKET = "inkless"
    MINIO_ACCESS_KEY = "minioadmin"
    MINIO_SECRET_KEY = "minioadmin"
    # rsm.config.key.prefix in services/kafka/templates/kafka.properties. Diskless
    # WAL objects, by contrast, are written at the bucket root as UUID keys
    # (inkless.object.key.prefix defaults to "").
    TIERED_PREFIX = "tiered-storage/"

    PG_HOST = "postgres"
    PG_PORT = 5432
    PG_DB = "inkless"
    PG_USER = "admin"
    PG_PASSWORD = "admin"

    def __init__(self, kafka):
        self.kafka = kafka
        self.logger = kafka.logger
        self._mc_alias_ready = False

    # ------------------------------------------------------------------ JMX --

    @classmethod
    def aggregate_object_name(cls, name):
        # Property order here is irrelevant for matching: JmxTool resolves the
        # MBean regardless of the order passed to --object-name, and the CSV
        # column it emits uses the broker's *registered* order (type,name -- see
        # find_aggregate_key). We just need a valid object name for --object-name.
        return "%s:type=%s,name=%s" % (cls.JMX_PACKAGE, cls.JMX_TYPE, name)

    @classmethod
    def partition_object_name(cls, name, topic, partition):
        # JMX sanitizes dots in tag values; topics here avoid dots so the raw
        # name works. Adjust if you test topics with dots in the name.
        return "%s:type=%s,name=%s,topic=%s,partition=%d" % (
            cls.JMX_PACKAGE, cls.JMX_TYPE, name, topic, partition)

    @classmethod
    def aggregate_object_names(cls):
        """Object names to pass to ``KafkaService(jmx_object_names=...)`` so the
        broker JmxTool starts polling the consolidation gauges at startup."""
        return [cls.aggregate_object_name(n)
                for n in (cls.TOTAL_LAG, cls.LOCAL_LAG, cls.DELETABLE_MESSAGES)]

    @classmethod
    def find_aggregate_key(cls, keys, name):
        """Find the ``maximum_jmx_value``/CSV key for the broker-aggregate gauge
        ``name``, tolerating any key-property ordering.

        JmxTool emits CSV columns as ``<ObjectName.toString()>:<attribute>``, and
        ObjectName.toString() preserves the order the MBean was *registered* with
        (here ``type=ConsolidationMetrics,name=...``) rather than sorting. Rather
        than hard-code that order (and silently read 0 if it ever changes), we
        match on the stable parts: the domain, the ``name=<name>`` token at a
        property boundary, the ``:Value`` attribute suffix, and the absence of
        topic/partition tags (so we pick the aggregate, not a per-partition
        gauge). Returns the matching key, or ``None`` if absent.
        """
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
        """Start the broker JmxTool on the consolidation gauges.

        Deliberately NOT wired into ``KafkaService(jmx_object_names=...)``: that
        propagates to every node including the isolated KRaft controller, which
        has no ReplicaManager and therefore never exposes the
        ``io.aiven.inkless.consolidation`` MBeans -- JmxTool's ``--wait`` then
        blocks and broker/controller startup times out.

        Call this AFTER the consolidating topic is active (some records produced)
        so the gauges are registered on the broker that leads the partition.
        ``KafkaService`` extends ``JmxMixin``/``KafkaPathResolverMixin``, so we
        reuse its ``start_jmx_tool`` (and the CSV parsing in ``read_jmx_output``).
        """
        self.kafka.jmx_object_names = self.aggregate_object_names()
        self.kafka.jmx_attributes = [self.JMX_ATTRIBUTE]
        for node in self.kafka.nodes:
            self.kafka.start_jmx_tool(self.kafka.idx(node), node)

    def latest_jmx_values(self):
        """Return the most recent sample of every monitored JMX attribute,
        summed across broker nodes.

        KafkaService starts JmxTool on each broker at startup (writing CSV to
        ``kafka.jmx_tool_log``). We read the header plus the last data line on
        each broker and sum per metric. Summing is correct for the aggregate
        gauges: only the broker that leads the consolidating partition reports a
        non-zero value, the rest report 0.
        """
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
        # Header: "time","<objectName>:<attr>",...   Data: <ms>,<v1>,<v2>,...
        names = [tok.strip().strip('"') for tok in header[0].strip().split(",")]
        data_line = last[0].strip()
        if not data_line or data_line.startswith('"'):
            # Only the header has been written so far.
            return {}
        fields = data_line.split(",")
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

    # ----------------------------------------------------------- Object store --

    def _storage_node(self):
        # Any broker node is on ducknet and resolves the `storage` alias.
        return self.kafka.nodes[0]

    def _ensure_mc_alias(self, node):
        if self._mc_alias_ready:
            return
        node.account.ssh(
            "mc alias set %s %s %s %s" % (
                self.MINIO_ALIAS, self.MINIO_ENDPOINT,
                self.MINIO_ACCESS_KEY, self.MINIO_SECRET_KEY),
            allow_fail=True)
        self._mc_alias_ready = True

    def object_keys(self):
        """Return all object keys in the inkless bucket (relative to the bucket
        root), via ``mc ls --recursive --json``."""
        node = self._storage_node()
        self._ensure_mc_alias(node)
        cmd = "mc ls --recursive --json %s/%s" % (self.MINIO_ALIAS, self.MINIO_BUCKET)
        keys = []
        for line in node.account.ssh_capture(cmd, allow_fail=True):
            line = line.strip()
            if not line:
                continue
            try:
                obj = json.loads(line)
            except ValueError:
                continue
            if obj.get("status") != "success":
                continue
            key = obj.get("key")
            if key:
                keys.append(key)
        return keys

    def tiered_object_count(self):
        return len([k for k in self.object_keys() if k.startswith(self.TIERED_PREFIX)])

    def wal_object_count(self):
        """Objects that are NOT under the tiered-storage prefix, i.e. diskless
        WAL files written at the bucket root."""
        return len([k for k in self.object_keys() if not k.startswith(self.TIERED_PREFIX)])

    # ----------------------------------------------------------- Control plane --

    def _psql(self, sql):
        node = self._storage_node()
        cmd = "PGPASSWORD='%s' psql -h %s -p %d -U %s -d %s -tAc \"%s\"" % (
            self.PG_PASSWORD, self.PG_HOST, self.PG_PORT, self.PG_USER, self.PG_DB, sql)
        return "".join(node.account.ssh_capture(cmd, allow_fail=False)).strip()

    def _psql_int(self, sql, default=0):
        out = self._psql(sql)
        if out == "" or out is None:
            return default
        try:
            return int(out.splitlines()[0].strip())
        except (ValueError, IndexError):
            return default

    def wal_batch_count(self, topic):
        """Number of WAL batch rows still tracked by the control plane for the
        topic. Drops toward 0 as the pruner deletes batches whose offsets are at
        or below the highest tiered offset."""
        return self._psql_int(
            "SELECT count(*) FROM batches b JOIN logs l ON b.topic_id = l.topic_id "
            "WHERE l.topic_name = '%s'" % topic)

    def min_log_start_offset(self, topic):
        """Minimum log_start_offset across the topic's partitions. Advances past
        0 once the WAL prefix is pruned."""
        return self._psql_int(
            "SELECT coalesce(min(log_start_offset), 0) FROM logs WHERE topic_name = '%s'" % topic)

    def high_watermark(self, topic):
        return self._psql_int(
            "SELECT coalesce(max(high_watermark), 0) FROM logs WHERE topic_name = '%s'" % topic)

    # --------------------------------------------------------------- Tooling --

    def verify_tooling(self):
        """Fail fast with an actionable message if the ducker image predates the
        addition of the mc/psql clients."""
        node = self._storage_node()
        missing = []
        for tool in ("mc", "psql"):
            if node.account.ssh("command -v %s" % tool, allow_fail=True) != 0:
                missing.append(tool)
        assert not missing, (
            "Missing client tool(s) %s on the broker node. Rebuild the ducker image "
            "(tests/docker/Dockerfile installs `mc` and `postgresql-client`) with "
            "`tests/docker/ducker-ak up` before running consolidation pipeline tests." % missing)
