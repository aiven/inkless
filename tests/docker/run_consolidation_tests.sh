#!/usr/bin/env bash
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

set -xeuo pipefail

# External dependencies for inkless: a Postgres control plane and a MinIO (S3) backend.
# ducker-ak only manages the `ducker*` nodes on `ducknet`; it has no hook to start these,
# so we bring them up here as standalone containers attached to `ducknet` with the network
# aliases (`postgres`, `storage`) that the broker properties template expects.
PG_CONTAINER="inkless-systest-postgres"
MINIO_CONTAINER="inkless-systest-storage"

ensure_deps() {
  docker network inspect ducknet >/dev/null 2>&1 || die "ducknet not found; run 'tests/docker/ducker-ak up' first"

  if ! docker inspect "${PG_CONTAINER}" >/dev/null 2>&1; then
    docker run -d --name "${PG_CONTAINER}" --network ducknet --network-alias postgres \
      -e POSTGRES_DB=inkless -e POSTGRES_USER=admin -e POSTGRES_PASSWORD=admin \
      postgres:17.2
  fi

  if ! docker inspect "${MINIO_CONTAINER}" >/dev/null 2>&1; then
    docker run -d --name "${MINIO_CONTAINER}" --network ducknet --network-alias storage \
      quay.io/minio/minio server /data --console-address ":9001"
  fi

  # Wait for Postgres, then ensure the `inkless` bucket exists in MinIO.
  until docker exec "${PG_CONTAINER}" pg_isready --dbname=inkless -U admin >/dev/null 2>&1; do
    echo "waiting for postgres"; sleep 2;
  done
  docker run --rm --network ducknet --entrypoint /bin/sh quay.io/minio/mc -c '
    until /usr/bin/mc alias set local http://storage:9000 minioadmin minioadmin; do
      echo "waiting for minio"; sleep 2;
    done;
    /usr/bin/mc mb -p local/inkless
  '
}

die() { echo "$@"; exit 1; }

ensure_deps

TESTS=(
#  tests/kafkatest/tests/core/replication_test.py
#  tests/kafkatest/tests/core/round_trip_fault_test.py
#  tests/kafkatest/tests/core/produce_bench_test.py::ProduceBenchTest.test_produce_bench
#  tests/kafkatest/tests/core/consume_bench_test.py
#  tests/kafkatest/tests/core/get_offset_shell_test.py
  tests/kafkatest/tests/client/consumer_test.py
#  tests/kafkatest/tests/client/compression_test.py
#  tests/kafkatest/tests/tools/log_compaction_test.py
#  tests/kafkatest/tests/streams/streams_smoke_test.py
#  tests/kafkatest/tests/streams/streams_eos_test.py
#  tests/kafkatest/tests/streams/streams_broker_down_resilience_test.py
#  tests/kafkatest/tests/connect/connect_distributed_test.py
)

bash tests/docker/ducker-ak test "${TESTS[@]}" -- --globals tests/docker/consolidation_globals.json