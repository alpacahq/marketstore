#!/bin/bash

# -----
# This test file is for the marketstore CLI features
# especially through a remote marketstore connection using the following command:
# marketstore connect --url {remote_marketstore_hostname}:{port}
# -----

function exit_if_failed() {
  if [[ $1 -ne 0 ]]; then
    echo "Failed"
    exit 1
  fi
}

function destroy_bucket() {
  hostAndPort=$1
  bucket=$2
  marketstore connect --url "${hostAndPort}" <<-EOF
  \destroy ${bucket}
EOF
}

function import_csv_remote() {
  hostAndPort=$1
  bucket=$2
  schema=$3
  record_type=$4
  csvfile=$5
  yamlfile=$6
  expected_file=$7

  marketstore connect --url ${hostAndPort} <<-EOF
  \create ${bucket}:Symbol/Timeframe/AttributeGroup ${schema} ${record_type}
  \getinfo ${bucket}
  \load ${bucket} ${csvfile} ${yamlfile}
  \o test_remote.csv
  \show ${bucket} 1970-01-01
EOF

  diff -q ${expected_file} test_remote.csv && echo "Passed"
  exit_if_failed $?
}

function cleanup() {
  hostAndPort=$1

  # https://github.com/koalaman/shellcheck/wiki/SC2181
  if ! rm -rf test_remote.csv testdata-remote; then
    echo "Failed: cannot delete test data and directory test_remote.csv testdata-remote"
    exit 1
  fi

}

#---------
MARKETSTORE_HOSTNAME=127.0.0.1:5993

cleanup $MARKETSTORE_HOSTNAME
destroy_bucket $MARKETSTORE_HOSTNAME TEST/1D/TICK
destroy_bucket $MARKETSTORE_HOSTNAME TEST2/1Min/TICK
destroy_bucket $MARKETSTORE_HOSTNAME TEST3/1Min/TICK
destroy_bucket $MARKETSTORE_HOSTNAME TEST4/1Sec/TICK

echo "--- import Tick csv1 ---"
import_csv_remote $MARKETSTORE_HOSTNAME TEST/1D/TICK Bid,Ask/float32 variable \
  bin/ticks-example-1.csv bin/ticks-example-1.yaml \
  bin/ticks-example-1-output-1D.csv
cleanup $MARKETSTORE_HOSTNAME
destroy_bucket $MARKETSTORE_HOSTNAME TEST/1D/TICK


echo "--- import Tick csv2 ---"
import_csv_remote $MARKETSTORE_HOSTNAME TEST2/1Min/TICK Bid,Ask/float32 variable \
  bin/ticks-example-2.csv bin/ticks-example-2.yaml \
  bin/ticks-example-2-output.csv
cleanup $MARKETSTORE_HOSTNAME
destroy_bucket $MARKETSTORE_HOSTNAME TEST2/1Min/TICK


echo "--- import Tick csv3 ---"
import_csv_remote $MARKETSTORE_HOSTNAME TEST3/1Min/TICK Bid,Ask/float32 variable \
  bin/ticks-example-not-sorted-by-time.csv bin/ticks-example-not-sorted-by-time.yaml \
  bin/ticks-example-not-sorted-by-time-output.csv
cleanup $MARKETSTORE_HOSTNAME
destroy_bucket $MARKETSTORE_HOSTNAME TEST3/1Min/TICK


echo "--- import Tick csv4 ---"
import_csv_remote $MARKETSTORE_HOSTNAME TEST4/1Sec/TICK Memo/string16:Num/int64 variable \
  bin/example-string.csv bin/example-string.yaml \
  bin/example-string-output.csv
cleanup $MARKETSTORE_HOSTNAME
destroy_bucket $MARKETSTORE_HOSTNAME TEST4/1Sec/TICK
