#!/bin/bash

sudo mkdir -p ${ANTITHESIS_OUTPUT_DIR}/logs && sudo chown ducker:ducker ${ANTITHESIS_OUTPUT_DIR}/logs

sudo service ssh start

set -Eeuo pipefail

exec "$@"
