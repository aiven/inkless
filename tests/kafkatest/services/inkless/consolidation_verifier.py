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

    # ducknet aliases of the external inkless dependencies, keyed by the logical
    # name a test uses to fault them (see block_dependency / heal_dependency).
    DEP_ALIASES = {"object_storage": "storage", "control_plane": "postgres"}

    def __init__(self, kafka):
        self.kafka = kafka
        self.logger = kafka.logger
        self._mc_alias_ready = False
        # node hostname -> {dependency: DROP'd IP}, so heal/teardown remove exactly the
        # rules block_dependency added.
        self._blocked = {}

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
        # Any broker resolves the postgres/storage aliases on ducknet. During an
        # outage the test only queries the dependency that is still up, so the default
        # broker is always reachable for the query at hand.
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

    def _object_entries(self, prefix=""):
        # (key, size) tuples under the given bucket prefix, via mc ls --recursive
        # --json. mc emits one JSON object per line with a "size" field in bytes.
        node = self._storage_node()
        self._ensure_mc_alias(node)
        target = "%s/%s" % (self.MINIO_ALIAS, self.MINIO_BUCKET)
        if prefix:
            target += "/" + prefix
        cmd = "mc ls --recursive --json %s" % target
        entries = []
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
                entries.append((key, int(obj.get("size") or 0)))
        return entries

    def object_keys(self, prefix=""):
        # Object keys under the given bucket prefix, via mc ls --recursive --json.
        return [key for key, _ in self._object_entries(prefix)]

    def tiered_object_count(self):
        # Count under the tiered-storage prefix only (scanning the whole bucket
        # in wait_until would slow and flap). Not topic-scoped, and the prefix
        # accumulates across tests in one `ducker-ak test` run (the whole inkless
        # suite is one invocation). Use baseline + delta (`> baseline`);
        # absolute `== N` flaps with test order.
        return len(self.object_keys(self.TIERED_PREFIX))

    def tiered_object_bytes(self):
        # Total bytes under the tiered-storage prefix. Like tiered_object_count,
        # this is not topic-scoped and accumulates across tests in one
        # `ducker-ak test` run, so use it as a baseline + delta, never absolute.
        # A retention.bytes test uses it to pick a whole-log size limit relative
        # to the data that actually reached remote, instead of guessing at the
        # per-record on-disk size. Scoping the listing to the prefix already limits
        # the entries to tiered storage (mc returns keys relative to the prefix, so
        # no further key filtering is possible here anyway).
        return sum(size for _, size in self._object_entries(self.TIERED_PREFIX))

    def wal_object_count(self):
        # Bucket-root count (outside tiered-storage/), i.e. diskless WAL files.
        # Same accumulation caveats as tiered_object_count: baseline + delta
        # only, never absolute.
        return len([k for k in self.object_keys() if not k.startswith(self.TIERED_PREFIX)])

    def wait_for_tiered_count_stable(self, settle_samples=3, backoff_sec=5, timeout_sec=300):
        """Wait until the tiered-storage object count stops growing (held steady across
        ``settle_samples`` consecutive samples) and return that stable count.

        Consolidation/RLM keeps copying closed segments to remote after the WAL has
        drained locally, so a count snapshot taken right after ``wait_for_tiered_offset_at_least``
        can still be climbing. Tests that later assert the count *dropped* after a
        reclaim need a settled pre-reclaim peak, otherwise late tiering can mask the
        deletion. Under a long retention nothing removes tiered objects, so the count
        only grows and then plateaus."""
        state = {"last": -1, "stable": 0, "value": 0}

        def check():
            cur = self.tiered_object_count()
            state["stable"] = state["stable"] + 1 if cur == state["last"] else 0
            state["last"] = cur
            state["value"] = cur
            self.logger.info("Tiered-storage object count: %d (stable for %d samples)"
                             % (cur, state["stable"]))
            return state["stable"] >= settle_samples

        wait_until(check, timeout_sec=timeout_sec, backoff_sec=backoff_sec,
                   err_msg="Tiered-storage object count did not stabilize within %ds." % timeout_sec)
        return state["value"]

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

    def create_consolidated_topic(self, topic, num_partitions, replication_factor,
                                  min_isr=2, segment_bytes=2 * 1024 * 1024, segment_ms=5000):
        """Create a *born-consolidated* diskless topic: diskless + remote storage are
        enabled at creation (no classic phase, no switch, so no seal). This is accepted
        because the cluster runs with ``diskless.allow.from.classic.enable=true``, which
        also opens diskless+remote at create time. ``segment.bytes`` defaults to 2 MiB
        (> the 1 MiB batch ceiling so a consolidation append does not trip
        ``RecordBatchTooLargeException``) and ``segment.ms`` to 5s, so a produced stream
        rolls closed, tierable segments."""
        self.kafka.create_topic({
            "topic": topic,
            "partitions": num_partitions,
            "replication-factor": replication_factor,
            "configs": {
                "min.insync.replicas": min_isr,
                "segment.bytes": segment_bytes,
                "segment.ms": segment_ms,
                "diskless.enable": "true",
                "remote.storage.enable": "true",
            },
        })

    def produce(self, topic, num_records, label="", timeout_sec=180, throughput=-1):
        """Produce ``num_records`` to ``topic`` with a one-shot ``VerifiableProducer``
        and return the acked count, asserting there were no worker errors. The
        producer node is freed before returning so the slot can be reused.

        ``throughput`` is records/sec (``-1`` = unthrottled). A finite rate spreads
        record timestamps over wall-clock time, which a ``retention.ms`` test needs
        so that only an aged prefix is eligible for deletion."""
        producer = VerifiableProducer(self.kafka.context, num_nodes=1, kafka=self.kafka,
                                      topic=topic, max_messages=num_records, throughput=throughput)
        producer.start()
        wait_until(lambda: producer.num_acked >= num_records or producer.worker_errors,
                   timeout_sec=timeout_sec, backoff_sec=1,
                   err_msg="Producer failed to produce %d %s records." % (num_records, label))
        assert not producer.worker_errors, "Producer errors (%s): %s" % (label, producer.worker_errors)
        producer.stop()
        acked = producer.num_acked
        producer.free()
        return acked

    def attempt_produce(self, topic, num_records, window_sec, label="outage"):
        """Best-effort produce that does NOT assert success. Runs a
        ``VerifiableProducer`` for up to ``window_sec`` (e.g. while a dependency is
        blocked), then SIGKILLs it and returns how many records got acked. The kill
        is deliberate, not a graceful stop: a clean shutdown would block flushing
        in-flight records against the dead dependency until ``delivery.timeout.ms``
        (~120s), and this producer is being abandoned. Worker errors are expected and
        not asserted on; callers use the acked count to check the broker does not ack
        writes it can't durably commit."""
        producer = VerifiableProducer(self.kafka.context, num_nodes=1, kafka=self.kafka,
                                      topic=topic, max_messages=num_records, throughput=-1)
        producer.start()
        deadline = time.time() + window_sec
        while time.time() < deadline and producer.num_acked < num_records:
            time.sleep(2)
        acked = producer.num_acked
        for node in producer.nodes:
            try:
                producer.kill_node(node, clean_shutdown=False, allow_fail=True)
            except Exception as e:  # noqa: BLE001 - producer may already be gone
                self.logger.warn("attempt_produce(%s): kill failed on %s: %s"
                                 % (label, node.account.hostname, e))
        producer.free()
        self.logger.info("attempt_produce(%s): acked %d/%d within %ds"
                         % (label, acked, num_records, window_sec))
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

    def _broker_bootstrap(self, node):
        """``hostname:port`` for a single broker's client listener, so a query can be
        pinned to one broker instead of the whole ``bootstrap_servers()`` list."""
        protocol = self.kafka.security_protocol
        port = self.kafka.port_mappings[protocol].port_number
        return "%s:%s" % (node.account.hostname, port)

    def offset_at(self, topic, time_spec, partition=0, bootstrap_server=None):
        """Offset for the partition at a ``kafka-get-offsets.sh --time`` spec
        (-1 latest, -2 earliest, -4 earliest-local, -5 latest-tiered). Returns -1 if
        not parseable yet.

        ``bootstrap_server`` pins the query to a single broker (see
        ``_broker_bootstrap``); by default the whole cluster is used. Pinning lets a
        test detect a per-broker divergence in the answer (e.g. ListOffsets(EARLIEST)
        served inconsistently across brokers)."""
        node = self.kafka.nodes[0]
        cmd = "%s --bootstrap-server %s --topic %s --partitions %d --time %s" % (
            self.kafka.path.script("kafka-get-offsets.sh", node),
            bootstrap_server or self.kafka.bootstrap_servers(),
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

    def wait_for_earliest_stable(self, topic, partition=0, settle_samples=3,
                                 backoff_sec=5, timeout_sec=240):
        """Wait until the *topic's* earliest readable offset
        (``kafka-get-offsets.sh --time -2``) has advanced past 0 and then held
        steady across ``settle_samples`` consecutive samples, and return that stable
        value.

        Unlike ``min_log_start_offset`` (the diskless WAL log-start in Postgres), the
        topic earliest reflects the whole log across tiers: it stays at 0 while the
        consolidated prefix ``[0, diskless_start)`` is still served from remote, and
        only advances once that prefix is actually deleted -- including removal of
        the remote segments below the new start. It is therefore the signal that
        cross-tier deletion has taken effect, not merely a WAL prune.

        Why *stable* and not just "above N": once the reclaim has finished, earliest
        should stop moving. Polling for a stable value avoids racing a consumer
        group (whose ``earliest`` reset can lag behind a still-advancing floor)
        and gives a trustworthy boundary for an explicit-offset read."""
        state = {"last": -1, "stable": 0, "value": 0}

        def check():
            cur = self.offset_at(topic, time_spec=-2, partition=partition)
            state["stable"] = state["stable"] + 1 if (cur > 0 and cur == state["last"]) else 0
            state["last"] = cur
            state["value"] = cur
            self.logger.info("Topic %s-%d earliest offset: %d (stable for %d samples)"
                             % (topic, partition, cur, state["stable"]))
            return state["stable"] >= settle_samples

        wait_until(check, timeout_sec=timeout_sec, backoff_sec=backoff_sec,
                   err_msg=("earliest offset for %s-%d did not advance past 0 and settle within "
                            "%ds; cross-tier deletion did not reach a stable floor"
                            % (topic, partition, timeout_sec)))
        return state["value"]

    def earliest_on_each_broker(self, topic, partition=0):
        """Query ListOffsets(EARLIEST) once against each broker individually and return
        a ``{hostname: offset}`` map. Before the ``DisklessFetchOffsetRouter`` fix a
        follower served ListOffsets(EARLIEST) from its frozen-at-switch local classic
        log while the leader served the advanced value, so the answer diverged per
        broker (Problem A). After the fix every broker routes to the control plane and
        returns the same cross-tier earliest."""
        result = {}
        for node in self.kafka.nodes:
            result[node.account.hostname] = self.offset_at(
                topic, time_spec=-2, partition=partition,
                bootstrap_server=self._broker_bootstrap(node))
        return result

    def wait_for_consistent_earliest_across_brokers(self, topic, partition=0,
                                                    settle_samples=3, backoff_sec=5,
                                                    timeout_sec=240):
        """Wait until ListOffsets(EARLIEST) is (a) > 0, (b) identical on every broker,
        and (c) held steady across ``settle_samples`` consecutive rounds; return that
        agreed value.

        This is the end-to-end Problem A assertion: the earliest must be one
        broker-agnostic value (the control-plane cross-tier earliest), not a per-broker
        answer that depends on which replica the metadata transformer routed the client
        to. A single positive value on every broker, stable over time, means the router
        no longer serves it from a replica's local classic log."""
        state = {"last": None, "stable": 0, "value": 0}

        def check():
            per_broker = self.earliest_on_each_broker(topic, partition=partition)
            values = set(per_broker.values())
            agreed = len(values) == 1 and all(v > 0 for v in values)
            cur = next(iter(values)) if agreed else None
            state["stable"] = state["stable"] + 1 if (agreed and cur == state["last"]) else 0
            state["last"] = cur
            if agreed:
                state["value"] = cur
            self.logger.info("Per-broker earliest for %s-%d: %s (agreed=%s, stable for %d samples)"
                             % (topic, partition, per_broker, agreed, state["stable"]))
            return state["stable"] >= settle_samples

        wait_until(check, timeout_sec=timeout_sec, backoff_sec=backoff_sec,
                   err_msg=("ListOffsets(EARLIEST) for %s-%d did not converge to a single "
                            "positive value across all brokers within %ds (Problem A: "
                            "per-broker earliest divergence)" % (topic, partition, timeout_sec)))
        return state["value"]

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

    def delete_records(self, topic, before_offset, partition=0):
        """Run ``kafka-delete-records.sh`` to delete every record before
        ``before_offset`` on ``topic``-``partition`` and return the resulting
        ``low_watermark`` (the new log start) for that partition.

        ``DeleteRecords`` takes a *beforeOffset*: the given offset becomes the new
        log start, so records ``[0, before_offset)`` are removed. On a consolidating
        topic the broker splits the request across tiers -- local ``UnifiedLog`` (which
        drives ``RemoteLogManager`` deletion of the remote segments below the new start)
        and, past the local log end, the diskless WAL in the control plane. The offset
        JSON file is written on the broker node and passed with ``--offset-json-file``.

        ``DeleteRecordsCommand`` prints the per-partition outcome on stdout as either
        ``partition: <tp>\\terror: <msg>`` or ``partition: <tp>\\tlow_watermark: <off>``.
        Success/failure is decided purely from that output, NOT from the process exit
        code: older ``kafka-delete-records.sh`` exits 0 even when a partition failed, so
        ``ssh_capture`` runs with ``allow_fail=True`` and we treat an ``error`` line for
        the target partition (or a missing ``low_watermark``) as failure. This keeps the
        test independent of whether the exit-code fix is present."""
        node = self.kafka.nodes[0]
        offset_json = ('{"partitions":[{"topic":"%s","partition":%d,"offset":%d}],"version":1}'
                       % (topic, partition, before_offset))
        json_path = "/tmp/delete-records-%s-%d.json" % (topic, partition)
        node.account.create_file(json_path, offset_json)
        cmd = "%s --bootstrap-server %s --offset-json-file %s" % (
            self.kafka.path.script("kafka-delete-records.sh", node),
            self.kafka.bootstrap_servers(),
            json_path,
        )
        self.logger.info("Deleting records before offset %d on %s-%d: %s"
                         % (before_offset, topic, partition, cmd))
        # allow_fail=True: don't rely on the exit code (older CLIs exit 0 on per-partition
        # failure); the per-partition result is parsed from stdout below instead.
        output = ""
        for line in node.account.ssh_capture(cmd, allow_fail=True):
            output += line.decode("utf-8") if isinstance(line, bytes) else line
        self.logger.info("kafka-delete-records.sh output:\n%s" % output)

        target = "%s-%d" % (topic, partition)
        low_watermark = None
        for line in output.splitlines():
            line = line.strip()
            err = re.search(r"partition:\s*(\S+)\s+error:\s*(.+)$", line)
            if err and err.group(1) == target:
                raise AssertionError(
                    "DeleteRecords before offset %d on %s failed: %s"
                    % (before_offset, target, err.group(2)))
            lw = re.search(r"partition:\s*(\S+)\s+low_watermark:\s*(-?\d+)$", line)
            if lw and lw.group(1) == target:
                low_watermark = int(lw.group(2))
        assert low_watermark is not None, (
            "DeleteRecords before offset %d on %s returned no low_watermark; output was:\n%s"
            % (before_offset, target, output))
        self.logger.info("DeleteRecords on %s returned low_watermark=%d" % (target, low_watermark))
        return low_watermark

    # --------------------- Dependency outage (iptables) ---------------------
    #
    # The object store ("storage") and control plane ("postgres") are standalone
    # ducknet containers, not ducker-managed services, so a test can't docker-pause
    # them. Instead we blackhole each broker's traffic to the dependency IP with an
    # iptables OUTPUT DROP (Trogdor's network-partition primitive). DROP, not REJECT,
    # so connections hang rather than get refused. The WAL and the tiered-storage RSM
    # share the "storage" container, so blocking it is a full object-storage outage.
    # Block only one dependency at a time so verifier queries can still reach the other.

    def _dep_alias(self, dependency):
        try:
            return self.DEP_ALIASES[dependency]
        except KeyError:
            raise AssertionError("Unknown dependency %r; expected one of %s"
                                 % (dependency, sorted(self.DEP_ALIASES)))

    def _resolve_alias_ip(self, node, alias):
        # Resolve via Docker's embedded DNS (127.0.0.11), which is broker-local and
        # therefore unaffected by a DROP rule on the dependency's own IP.
        out = "".join(node.account.ssh_capture("getent hosts %s" % alias, allow_fail=False))
        parts = out.split()
        assert parts, "Could not resolve ducknet alias '%s' on %s" % (alias, node.account.hostname)
        return parts[0]

    def block_dependency(self, dependency):
        """Simulate an outage of ``dependency`` ("object_storage" or
        "control_plane") by DROP'ing every broker's traffic to its resolved
        ducknet IP. Idempotent per node (guarded with ``iptables -C``)."""
        alias = self._dep_alias(dependency)
        for node in self.kafka.nodes:
            ip = self._resolve_alias_ip(node, alias)
            node.account.ssh(
                "sudo iptables -C OUTPUT -d %s -j DROP 2>/dev/null || "
                "sudo iptables -A OUTPUT -d %s -j DROP" % (ip, ip), allow_fail=False)
            self._blocked.setdefault(node.account.hostname, {})[dependency] = ip
        self.logger.info("Blocked dependency %s (alias %s) on all brokers" % (dependency, alias))

    def heal_dependency(self, dependency):
        """Remove the exact DROP rule :meth:`block_dependency` added for ``dependency``
        on every broker, restoring connectivity. Deletes the recorded IP rather than
        re-resolving, so the rule still lifts even if the alias's IP changed."""
        for node in self.kafka.nodes:
            ip = self._blocked.get(node.account.hostname, {}).pop(dependency, None)
            if ip is not None:
                self._drop_rule(node, ip)
        self.logger.info("Healed dependency %s on all brokers" % dependency)

    def heal_all(self):
        """Teardown safety net: flush every DROP rule this verifier ever added so a
        failed assertion cannot leave a broker partitioned for later tests."""
        for node in self.kafka.nodes:
            for ip in list(self._blocked.get(node.account.hostname, {}).values()):
                self._drop_rule(node, ip)
        self._blocked = {}

    @staticmethod
    def _drop_rule(node, ip):
        # Delete the rule repeatedly in case it was added more than once.
        node.account.ssh(
            "while sudo iptables -C OUTPUT -d %s -j DROP 2>/dev/null; do "
            "sudo iptables -D OUTPUT -d %s -j DROP; done" % (ip, ip), allow_fail=True)

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
