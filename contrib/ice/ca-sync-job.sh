#!/bin/bash -x

set -eEuo pipefail

log_file="${CA_MKTSDB_PATH}/ingest-${now}.log"

ca_importer \
  -data_dir "${CA_MKTSDB_PATH}"
  -reorg_dir "${CA_REORG_PATH}"
  -reimport "${CA_REIMPORT}" > "${log_file}"

echo "Triggering marketstore restart"
kill $(pidof marketstore)
