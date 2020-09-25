#!/bin/bash -x

set -eEuo pipefail

from=$(date -d "-${BACKFILL_INTERVAL}" "+%Y-%m-%d")
now=$(date +%Y%m%d_%H%M%S)
log_file="${BACKFILL_MKTSDB_PATH}/ingest-${now}.log"

polygon_backfiller \
  -from "${from}" \
  -parallelism "${BACKFILL_PARALLELISM}" \
  ${BACKFILL_ARGS} \
  -apiKey "${BACKFILL_APIKEY}" \
  -dir "${BACKFILL_MKTSDB_PATH}" > "${log_file}"

echo "Triggering marketstore restart"
kill $(pidof marketstore)
