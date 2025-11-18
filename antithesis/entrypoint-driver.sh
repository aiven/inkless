#!/bin/bash

# Let the Antithesis runtime know we're ready to start the chaos.
# See here https://antithesis.com/docs/getting_started/setup/#ready-signal
echo '{"antithesis_setup": { "status": "complete", "details": null }}' > $ANTITHESIS_OUTPUT_DIR/sdk.jsonl

set -Eeuo pipefail

exec "$@"
