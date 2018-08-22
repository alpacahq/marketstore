#!/usr/bin/env bash
set -e

rm -r /project/data/mktsdb/* || echo
wget $1 -O /tmp/mktsdb.tar.gz
tar -xzvf /tmp/mktsdb.tar.gz -C /project/data/
rm /tmp/mktsdb.tar.gz