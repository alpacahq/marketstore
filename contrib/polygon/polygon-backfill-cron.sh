#!/bin/bash -x

set -eEuo pipefail

export BACKFILL_SCHEDULE="${BACKFILL_SCHEDULE:=0 0 1 * * *}"
export BACKFILL_PARALLELISM="${BACKFILL_PARALLELISM:=5}"

export BACKFILL_LIVE_DIR="${BACKFILL_LIVE_DIR:=/data/live}"
export BACKFILL_OLD_DIR="${BACKFILL_OLD_DIR:=/data/old}"
export BACKFILL_TEMP_DIR="${BACKFILL_TEMP_DIR:=/data/temp}"
export BACKFILL_LOG_DIR="${BACKFILL_LOG_DIR:=/data/log}"

: "${BACKFILL_SCHEDULE:=0 0 1 * * *}"

mkdir -p ${BACKFILL_LIVE_DIR}
mkdir -p ${BACKFILL_OLD_DIR}
mkdir -p ${BACKFILL_TEMP_DIR}
mkdir -p ${BACKFILL_LOG_DIR}

backfill_job="$(dirname ${0})/polygon-backfill-job.sh"

curl -L https://github.com/odise/go-cron/releases/download/v0.0.7/go-cron-linux.gz | zcat > /usr/local/bin/go-cron
chmod u+x /usr/local/bin/go-cron

[ -z "$BACKFILL_APIKEY" ] && echo "BACKFILL_APIKEY env not set" && exit 1

/usr/local/bin/go-cron \
  -s="${BACKFILL_SCHEDULE}" \
  -- \
  /bin/bash -cx \
  ${backfill_job}