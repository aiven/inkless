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
import re
import shlex
import time

from ducktape.utils.util import wait_until

from kafkatest.services.verifiable_producer import VerifiableProducer


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

    # Switch-completion JMX gauges (broker-side): on a classic-to-diskless switch the
    # config flips to diskless.enable=true before leaders seal and before
    # InitDisklessLogManager commits the diskless start, so tests gate on these, not
    # config visibility. Wire them into the KafkaService constructor (the controller
    # never exposes them, so scraping it would hang start_jmx_tool's --wait).
    SEALED_LEADER_PARTITIONS_JMX_OBJECT = "kafka.server:type=ReplicaManager,name=SealedPartitionsCount"
    INIT_DISKLESS_IN_FLIGHT_PARTITIONS_JMX_OBJECT = \
        "kafka.server:type=InitDisklessLogManager,name=InFlightPartitions"
    SWITCH_JMX_OBJECT_NAMES = [
        SEALED_LEADER_PARTITIONS_JMX_OBJECT,
        INIT_DISKLESS_IN_FLIGHT_PARTITIONS_JMX_OBJECT,
    ]
    SWITCH_JMX_ATTRIBUTES = ["Value"]
    SWITCH_TIMEOUT_SEC = 180

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
        # ssh_capture may yield bytes on some ducktape versions; csv.reader requires
        # str, so decode here to match the verifier's other ssh_capture callers
        # (offset_at, read_contiguous_from, read_records_with_values_from).
        header_line = header[0]
        header_line = header_line.decode("utf-8") if isinstance(header_line, bytes) else header_line
        data_line = last[0]
        data_line = data_line.decode("utf-8") if isinstance(data_line, bytes) else data_line
        # Parse with csv: JMX ObjectNames contain commas (property separators) and
        # are quoted in the header, so a naive comma split would misalign column
        # names with their values. Header: "time","<objectName>:<attr>",...
        names = next(csv.reader([header_line.strip()]), [])
        data_line = data_line.strip()
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
        # Count under the tiered-storage prefix only (scanning the whole bucket
        # in wait_until would slow and flap). Not topic-scoped, and the prefix
        # accumulates across tests in one `ducker-ak test` run (the whole inkless
        # suite is one invocation). Use baseline + delta (`> baseline`);
        # absolute `== N` flaps with test order.
        return len(self.object_keys(self.TIERED_PREFIX))

    def wal_object_count(self):
        # Bucket-root count (outside tiered-storage/), i.e. diskless WAL files.
        # Same accumulation caveats as tiered_object_count: baseline + delta
        # only, never absolute.
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

    # --------------------- Topic / produce ---------------------

    def create_classic_topic(self, topic, num_partitions, replication_factor,
                             min_isr=2, segment_bytes=1048576, segment_ms=5000):
        """Create a plain classic (non-diskless, non-tiered) topic with a small
        ``segment.bytes`` so a produced prefix rolls several closed segments.
        diskless/remote are left unset (the cluster default is classic); remote is
        enabled only later by the consolidation transition, so segments tier (with
        their classic epochs) only once consolidation starts. ``segment.bytes``
        defaults to the enforced 1 MiB minimum and ``segment.ms`` to 5s so records
        flow through closed segments before the size floor is reached."""
        self.kafka.create_topic({
            "topic": topic,
            "partitions": num_partitions,
            "replication-factor": replication_factor,
            "configs": {
                "min.insync.replicas": min_isr,
                "segment.bytes": segment_bytes,
                "segment.ms": segment_ms,
            },
        })

    def produce(self, topic, num_records, label="", timeout_sec=180):
        """Produce ``num_records`` to ``topic`` with a one-shot ``VerifiableProducer``
        and return the acked count, asserting there were no worker errors. The
        producer node is freed before returning so the slot can be reused."""
        producer = VerifiableProducer(self.kafka.context, num_nodes=1, kafka=self.kafka,
                                      topic=topic, max_messages=num_records, throughput=-1)
        producer.start()
        wait_until(lambda: producer.num_acked >= num_records or producer.worker_errors,
                   timeout_sec=timeout_sec, backoff_sec=1,
                   err_msg="Producer failed to produce %d %s records." % (num_records, label))
        assert not producer.worker_errors, "Producer errors (%s): %s" % (label, producer.worker_errors)
        producer.stop()
        acked = producer.num_acked
        producer.free()
        return acked

    # --------------------- Switch completion (JMX) ---------------------

    def wait_for_switch_complete(self, topic, num_partitions, timeout_sec=None):
        """Wait until the classic-to-diskless init work has drained. The config turns
        visible before leaders seal and before InitDisklessLogManager commits the
        diskless start, so gate on broker-side JMX (sealed leader partitions present,
        zero in-flight init partitions) to keep a post-switch produce from racing the
        switch-pending state. Requires the KafkaService to be wired with
        ``SWITCH_JMX_OBJECT_NAMES``/``SWITCH_JMX_ATTRIBUTES``."""
        if timeout_sec is None:
            timeout_sec = self.SWITCH_TIMEOUT_SEC

        wait_until(lambda: self.diskless_config_visible(topic),
                   timeout_sec=timeout_sec, backoff_sec=2,
                   err_msg="Topic %s did not show diskless.enable=true within %ds"
                           % (topic, timeout_sec))

        expected_sealed = num_partitions

        def check():
            try:
                self.kafka.read_jmx_output_all_nodes()
                sealed_key = "%s:Value" % self.SEALED_LEADER_PARTITIONS_JMX_OBJECT
                in_flight_key = "%s:Value" % self.INIT_DISKLESS_IN_FLIGHT_PARTITIONS_JMX_OBJECT
                sealed = 0.0
                in_flight = 0.0
                for time_to_stats in self.kafka.jmx_stats:
                    if time_to_stats:
                        latest = max(time_to_stats.keys())
                        sealed += time_to_stats[latest].get(sealed_key, 0)
                        in_flight += time_to_stats[latest].get(in_flight_key, 0)
                self.logger.info("Switch state for %s: sealed leader partitions=%d/%d, "
                                 "in-flight init partitions=%d"
                                 % (topic, int(sealed), expected_sealed, int(in_flight)))
                return sealed >= expected_sealed and in_flight == 0
            except Exception as e:  # noqa: BLE001 - JMX reads race broker startup
                self.logger.debug("Failed to read switch JMX state for %s: %s" % (topic, e))
                return False

        wait_until(check, timeout_sec=timeout_sec, backoff_sec=5,
                   err_msg=("Topic %s did not finish the classic-to-diskless switch within %ds "
                            "(expected >= %d sealed leader partitions and zero in-flight init "
                            "partitions)" % (topic, timeout_sec, expected_sealed)))

    def diskless_config_visible(self, topic):
        try:
            return self.kafka.describe_topic_config(topic).get("diskless.enable") == "true"
        except Exception:  # noqa: BLE001 - describe can race the config propagation
            return False

    # --------------------- Wipe / offsets / reads ---------------------

    def wipe_topic_local_logs(self, node, topic):
        """Delete only the topic's partition dirs from both data dirs, keeping the
        broker's KRaft identity and internal topics (__cluster_metadata,
        __remote_log_metadata, __consumer_offsets) so it can rebuild the partition
        and locate the remote segments. ``allow_fail``: a data dir may not hold a
        replica of this partition."""
        for data_dir in (self.kafka.DATA_LOG_DIR_1, self.kafka.DATA_LOG_DIR_2):
            node.account.ssh("rm -rf -- %s/%s-*" % (data_dir, topic), allow_fail=True)
        self.logger.info("Wiped local logs for %s on %s" % (topic, node.account.hostname))

    def partition_has_leader(self, topic, partition=0):
        try:
            return self.kafka.leader(topic, partition=partition) is not None
        except Exception as e:  # noqa: BLE001 - leader lookup races broker startup
            self.logger.debug("Leader lookup for %s-%d not ready yet: %s" % (topic, partition, e))
            return False

    def offset_at(self, topic, time_spec, partition=0):
        """Offset for the partition at a ``kafka-get-offsets.sh --time`` spec
        (-1 latest, -2 earliest, -4 earliest-local, -5 latest-tiered). Returns -1 if
        not parseable yet."""
        node = self.kafka.nodes[0]
        cmd = "%s --bootstrap-server %s --topic %s --partitions %d --time %s" % (
            self.kafka.path.script("kafka-get-offsets.sh", node),
            self.kafka.bootstrap_servers(),
            topic,
            partition,
            time_spec,
        )
        try:
            for line in node.account.ssh_capture(cmd, allow_fail=True):
                line = line.decode("utf-8") if isinstance(line, bytes) else line
                parts = line.strip().split(":")
                if len(parts) == 3 and parts[0] == topic and parts[1] == str(partition):
                    return int(parts[2])
        except Exception as e:  # noqa: BLE001 - best effort; tool may not be ready
            self.logger.warn("Failed to read offset (time=%s) for %s-%d: %s"
                             % (time_spec, topic, partition, str(e)))
        return -1

    def wait_for_offset(self, topic, time_spec, partition=0, timeout_sec=60):
        """Poll ``offset_at`` until it returns a real (>= 0) offset; offset reads race
        broker startup after a restart, so retry rather than fail on -1."""
        result = {"offset": -1}

        def _ready():
            result["offset"] = self.offset_at(topic, time_spec=time_spec, partition=partition)
            return result["offset"] >= 0

        wait_until(_ready, timeout_sec=timeout_sec, backoff_sec=2,
                   err_msg="Could not read offset (time=%s) for %s-%d."
                           % (time_spec, topic, partition))
        return result["offset"]

    def wait_for_local_log_truncation(self, topic, partition=0, timeout_sec=180):
        """Wait until earliest-local advances past 0 and return it; proves some closed
        segments were tiered AND locally deleted, so reads below this watermark come
        from remote."""
        result = {"offset": -1}

        def check():
            result["offset"] = self.offset_at(topic, time_spec=-4, partition=partition)
            self.logger.info("Topic %s-%d earliest-local offset: %d"
                             % (topic, partition, result["offset"]))
            return result["offset"] > 0

        wait_until(check, timeout_sec=timeout_sec, backoff_sec=2,
                   err_msg=("earliest-local offset for %s-%d did not advance past 0 within %ds; "
                            "classic tiering or local retention is not progressing"
                            % (topic, partition, timeout_sec)))
        return result["offset"]

    def wait_for_tiered_offset_at_least(self, topic, min_offset, partition=0, timeout_sec=180):
        """Wait until the latest-tiered offset (``kafka-get-offsets.sh --time -5``, i.e.
        the broker's ``highestOffsetInRemoteStorage``) reaches at least ``min_offset``
        and return it.

        This is a *durability* signal, used after a classic->diskless switch to prove the
        remote tier covers the requested range (here, through the seal). Once consolidation
        appends a diskless record, its higher diskless leader epoch rolls a fresh segment
        exactly at the seal, closing the boundary classic segment ``[.., seal)``; the
        classic RLM then copies it to remote. Reaching the seal therefore proves the whole
        classic prefix -- including the boundary tail -- is recoverable off-broker after a
        wipe.

        Deliberately NOT earliest-local (-4): that tracks deletion of the already-tiered
        *local* copies, which only happens on the periodic retention sweep
        (``log.retention.check.interval.ms``, 5min default) and is irrelevant to
        durability -- gating on it makes the test hostage to retention timing."""
        result = {"offset": -1}

        def check():
            result["offset"] = self.offset_at(topic, time_spec=-5, partition=partition)
            self.logger.info("Topic %s-%d latest-tiered offset: %d (need >= %d)"
                             % (topic, partition, result["offset"], min_offset))
            return result["offset"] >= min_offset

        wait_until(check, timeout_sec=timeout_sec, backoff_sec=2,
                   err_msg=("latest-tiered offset for %s-%d did not reach %d within %ds; "
                            "the classic prefix through the seal was not tiered to remote"
                            % (topic, partition, min_offset, timeout_sec)))
        return result["offset"]

    def read_contiguous_from(self, topic, from_offset=0, max_messages=1, partition=0,
                             timeout_ms=120000):
        """Fetch up to ``max_messages`` records from ``from_offset`` and return
        ``(first_offset, num_read)``; ``first_offset`` is -1 if nothing returns.
        Unlike kafka-get-offsets.sh (advertised offset), an actual fetch reveals
        whether the broker serves ``from_offset`` or skips ahead."""
        node = self.kafka.nodes[0]
        cmd = ("%s --bootstrap-server %s --topic %s --partition %d --offset %d "
               "--max-messages %d --timeout-ms %d "
               "--property print.offset=true --property print.key=false "
               "--property print.value=false" % (
                   self.kafka.path.script("kafka-console-consumer.sh", node),
                   self.kafka.bootstrap_servers(),
                   topic,
                   partition,
                   from_offset,
                   max_messages,
                   timeout_ms,
               ))
        first_offset = -1
        num_read = 0
        try:
            for line in node.account.ssh_capture(cmd, allow_fail=True):
                line = line.decode("utf-8") if isinstance(line, bytes) else line
                # We always pass print.offset=true, so each record line is exactly
                # "Offset:<n>". Anchor on that (or a digits-only line for legacy
                # output) so a number inside a stray log/summary line can't be
                # mistaken for an offset.
                stripped = line.strip()
                match = re.match(r"Offset:(\d+)$", stripped) or re.match(r"(\d+)$", stripped)
                if match:
                    if first_offset < 0:
                        first_offset = int(match.group(1))
                    num_read += 1
        except Exception as e:  # noqa: BLE001 - best effort; tool may time out
            self.logger.warn("Failed to read from %s-%d at offset %d: %s"
                             % (topic, partition, from_offset, str(e)))
        return first_offset, num_read

    def first_served_offset(self, topic, partition=0, from_offset=0, timeout_ms=120000):
        """Offset of the first record a real fetch from ``from_offset`` returns, or -1
        if nothing returns within the timeout."""
        first, _ = self.read_contiguous_from(topic, from_offset=from_offset,
                                             max_messages=1, partition=partition,
                                             timeout_ms=timeout_ms)
        return first

    def read_records_with_values_from(self, topic, from_offset, max_messages, partition=0,
                                      timeout_ms=120000):
        """Fetch up to ``max_messages`` records from ``from_offset`` and return them as a
        list of ``(offset, value)`` tuples in returned order.

        The VerifiableProducer default (``is_int``) writes each record's per-producer
        sequence number as the value, so callers can assert payload content -- not just
        offsets -- and detect gaps, duplicates, or reordering across the
        classic->diskless boundary. Each output line is ``Offset:<n>\\t<value>``:
        DefaultMessageFormatter prints the offset prefix, then key.separator '\\t', then
        the value."""
        node = self.kafka.nodes[0]
        cmd = ("%s --bootstrap-server %s --topic %s --partition %d --offset %d "
               "--max-messages %d --timeout-ms %d "
               "--property print.offset=true --property print.key=false "
               "--property print.value=true" % (
                   self.kafka.path.script("kafka-console-consumer.sh", node),
                   self.kafka.bootstrap_servers(),
                   topic,
                   partition,
                   from_offset,
                   max_messages,
                   timeout_ms,
               ))
        records = []
        try:
            for line in node.account.ssh_capture(cmd, allow_fail=True):
                line = line.decode("utf-8") if isinstance(line, bytes) else line
                match = re.match(r"Offset:(\d+)\s+(-?\d+)\s*$", line.strip())
                if match:
                    records.append((int(match.group(1)), int(match.group(2))))
        except Exception as e:  # noqa: BLE001 - best effort; tool may time out
            self.logger.warn("Failed to read records from %s-%d at offset %d: %s"
                             % (topic, partition, from_offset, str(e)))
        return records
