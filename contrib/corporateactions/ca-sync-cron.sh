#!/bin/bash -x

set -eEuo pipefail

export CA_MKTSDB_PATH="${CA_MKTSDB_PATH:=/data}"
export CA_REORG_PATH="${CA_REORG_PATH:=/reorg}"
export CA_REIMPORT="${CA_REIMPORT:=false}"
: "${CA_SCHEDULE:=0 0 1 * * *}"

ca_job="$(dirname ${0})/ca-sync-job.sh"

curl -L https://github.com/odise/go-cron/releases/download/v0.0.7/go-cron-linux.gz | zcat > /usr/local/bin/go-cron
chmod u+x /usr/local/bin/go-cron

/usr/local/bin/go-cron \
  -s="${CA_SCHEDULE}" \
  -- \
  /bin/bash -cx \
  ${ca_job}