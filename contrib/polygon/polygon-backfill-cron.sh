#!/bin/bash -x

set -eEuo pipefail

export BACKFILL_INTERVAL="${1}"  # e.g. '1 week ago'
export BACKFILL_APIKEY="${2}"
export BACKFILL_ARGS="${@:3}"
export BACKFILL_PARALLELISM="${BACKFILL_PARALLELISM:=5}"
export BACKFILL_MKTSDB_PATH="${BACKFILL_MKTSDB_PATH:=/data}"
: "${BACKFILL_SCHEDULE:=0 0 1 * * *}"

backfill_job="$(dirname ${0})/polygon-backfill-job.sh"

curl -L https://github.com/odise/go-cron/releases/download/v0.0.7/go-cron-linux.gz | zcat > /usr/local/bin/go-cron
chmod u+x /usr/local/bin/go-cron

/usr/local/bin/go-cron \
  -s="${BACKFILL_SCHEDULE}" \
  -- \
  /bin/bash -cx \
  ${backfill_job}