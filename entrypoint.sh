#!/bin/sh

uid=${MKTSDB_UID:-1000}

cat /etc/passwd | grep mktsdb
if [ $? -eq 0 ]; then
    deluser mktsdb
fi

adduser -D -g '' -u ${uid} -h /home/mktsdb mktsdb

sudo chown -R mktsdb:mktsdb /project

exec su-exec mktsdb "$@"