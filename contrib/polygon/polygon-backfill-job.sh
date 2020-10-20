#!/bin/bash -x

set -eEuo pipefail

tradesFrom=$(date -d "${BACKFILL_TRADE_INTERVAL}" "+%Y-%m-%d")
barsFrom=$(date -d "${BACKFILL_BAR_INTERVAL}" "+%Y-%m-%d")
to=$(date -d "-1 day ago" "+%Y-%m-%d")
now=$(date +%Y%m%d_%H%M%S)
tradeLogFile="${BACKFILL_LOG_DIR}/ingest-trade-${now}.log"
barLogFile="${BACKFILL_LOG_DIR}/ingest-bar-${now}.log"

echo "Delete non live data"
rm -rf ${BACKFILL_OLD_DIR}
rm -rf ${BACKFILL_TEMP_DIR}
mkdir -p ${BACKFILL_TEMP_DIR}

echo "Start trade backfill"

polygon_backfiller \
  -trades \
  -from "${tradesFrom}" \
  -to "${to}" \
  -parallelism "${BACKFILL_PARALLELISM}" \
  ${BACKFILL_TRADE_ARGS} \
  -apiKey "${BACKFILL_APIKEY}" \
  -dir "${BACKFILL_TEMP_DIR}" > "${tradeLogFile}"

echo "Start bar backfill"

polygon_backfiller \
  -bars \
  -from "${barsFrom}" \
  -to "${to}" \
  -parallelism "${BACKFILL_PARALLELISM}" \
  ${BACKFILL_BAR_ARGS} \
  -apiKey "${BACKFILL_APIKEY}" \
  -dir "${BACKFILL_TEMP_DIR}" > "${barLogFile}"

pid=$(pidof marketstore)

echo "Stopping marketstore"
kill -STOP $pid

echo "Move working folders"
mv ${BACKFILL_LIVE_DIR} ${BACKFILL_OLD_DIR}
mv ${BACKFILL_TEMP_DIR} ${BACKFILL_LIVE_DIR}

echo "Kill marketstore"
kill -9 $pid

echo "Backfill ended successfully"

