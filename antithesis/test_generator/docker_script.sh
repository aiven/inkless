#!/usr/bin/bash
set -ex

cd /tmp  # prevent log files from going outside of container

mkdir -p /workdir/antithesis/test/v1/
python /workdir/antithesis/test_generator/generate_tests.py \
  /workdir/tests/kafkatest/tests/inkless/inkless_produce_consume_test.py \
  /workdir/antithesis/test/v1/
