#!/bin/bash
echo test2
mkdir -p /project/data/mktsdb

# Create the first directory, then we have restart mkts (only needed for the very first subdirectory)
mkts -rootDir /project/data/mktsdb <<EOF
\create TEST/1Min/TICK:Symbol/Timeframe/AttributeGroup bid,ask/float32 variable
EOF

# Load data into the 1Min bucket, then create a 1H directory and load the same data into it
mkts -rootDir /project/data/mktsdb <<EOF
\load TEST/1Min /project/tests/setup_mkts/tick/2/ticks-example.csv /project/tests/setup_mkts/tick/2/ticks-example.yaml
\create TEST/1H/TICK:Symbol/Timeframe/AttributeGroup bid,ask/float32 variable
\load TEST/1H/TICK /project/tests/setup_mkts/tick/2/ticks-example.csv /project/tests/setup_mkts/tick/2/ticks-example.yaml
EOF

# Run a simple SQL query against the new data
mkts -rootDir /project/data/mktsdb <<EOF
select * from \`TEST/1H/TICK\` limit 10;
EOF