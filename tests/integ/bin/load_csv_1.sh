#!/bin/bash

echo test1
mkdir -p /project/data/mktsdb

mkts -rootDir /project/data/mktsdb <<EOF
\create TEST/1Min/TICK:Symbol/Timeframe/AttributeGroup Bid,Ask/float32 variable
EOF

mkts -rootDir /project/data/mktsdb <<EOF
\load TEST/1Min /project/tests/setup_mkts/tick/1/ticks-example.csv /project/tests/setup_mkts/tick/1/ticks-example.yaml
\create TEST/1H/TICK:Symbol/Timeframe/AttributeGroup Bid,Ask/float32 variable
\load TEST/1H /project/tests/setup_mkts/tick/1/ticks-example.csv /project/tests/setup_mkts/tick/1/ticks-example.yaml
EOF


# Run a simple SQL query against the new data
mkts -rootDir /project/data/mktsdb <<EOF
select * from \`TEST/1Min/TICK\` limit 10;
EOF

mkts -rootDir /project/data/mktsdb <<EOF
select * from \`TEST/1H/TICK\` limit 10;
EOF