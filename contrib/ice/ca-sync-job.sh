#!/bin/bash -x

set -eEuo pipefail

log_file="${CA_MKTSDB_PATH}/ingest-${now}.log"

ice reorg import \
  "${CA_MKTSDB_PATH}"
  "${CA_REORG_PATH}"
  -reimport "${CA_REIMPORT}" > "${log_file}"

echo "Triggering marketstore restart"
kill $(pidof marketstore)
