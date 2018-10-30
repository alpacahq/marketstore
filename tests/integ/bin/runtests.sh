#!/bin/bash

rm -rf testdata test_ticks.csv && mkdir -p testdata/mktsdb

marketstore connect -d `pwd`/testdata/mktsdb <<- EOF >& /dev/null && diff -q bin/ticks-example-1-output.csv test_ticks.csv && echo "Passed" || echo "Test Failed"
\create TEST/1Min/TICK:Symbol/Timeframe/AttributeGroup Bid,Ask/float32 variable
\getinfo TEST/1Min/TICK
\load TEST/1Min/TICK bin/ticks-example-1.csv bin/ticks-example-1.yaml
\o test_ticks.csv
\show TEST/1Min/TICK 1970-01-01
EOF

rm -f test_ticks.csv

marketstore connect -d `pwd`/testdata/mktsdb <<- EOF >& /dev/null && diff -q bin/ticks-example-2-output.csv test_ticks.csv && echo "Passed" || echo "Test Failed"
\create TEST/1Min/TICK:Symbol/Timeframe/AttributeGroup Bid,Ask/float32 variable
\getinfo TEST/1Min/TICK
\load TEST/1Min/TICK bin/ticks-example-2.csv bin/ticks-example-2.yaml
\o test_ticks.csv
\show TEST/1Min/TICK 1970-01-01
EOF

rm -rf testdata test_ticks.csv

