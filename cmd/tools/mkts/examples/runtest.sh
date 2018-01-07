#!/bin/bash

rm -rf testdir
mkdir testdir
# Create the first directory, then we have restart mkts (only needed for the very first subdirectory)
mkts -rootDir testdir <<EOF
\create TEST/1Min/TICK:Symbol/Timeframe/AttributeGroup bid,ask/float32 variable
EOF


# Load data into the 1Min bucket, then create a 1H directory and load the same data into it
mkts -rootDir testdir <<EOF
\load TEST/1Min ticks-example.csv ticks-example.yaml
\create TEST/1H/TICK:Symbol/Timeframe/AttributeGroup bid,ask/float32 variable
\load TEST/1H/TICK ticks-example.csv ticks-example.yaml
EOF

# Run a simple SQL query against the new data
mkts -rootDir testdir <<EOF
select * from \`TEST/1H/TICK\` limit 10;
EOF
