#!/usr/bin/env bash
set -e

if [ ! -d /project ]; then
	echo "Error: there is no /project directory mounted..."
	echo "exiting..."
	exit 1
fi

rm -rf /project/data/mktsdb/*
mkdir -p /project/data/mktsdb

wget $1 -O /tmp/mktsdb.tar.gz
tar -xzvf /tmp/mktsdb.tar.gz -C /project/data/

rm /tmp/mktsdb.tar.gz
