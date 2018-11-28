#!/bin/bash

rm -rf testdata test_ticks.csv && mkdir -p testdata/mktsdb
if [ $? -ne 0 ]; then exit 1; fi

marketstore connect -d `pwd`/testdata/mktsdb <<- EOF
\create TEST/24H/TICK:Symbol/Timeframe/AttributeGroup Bid,Ask/float32 variable
\getinfo TEST/24H/TICK
\load TEST/24H/TICK bin/ticks-example-1.csv bin/ticks-example-1.yaml
\o test_ticks.csv
\show TEST/24H/TICK 1970-01-01
EOF
if [ $? -ne 0 ]; then exit 1; fi

diff -q bin/ticks-example-1-output.csv test_ticks.csv && echo "Passed"
if [ $? -ne 0 ]; then exit 1; fi

rm -f test_ticks.csv
if [ $? -ne 0 ]; then exit 1; fi

marketstore connect -d `pwd`/testdata/mktsdb <<- EOF
\create TEST2/1Min/TICK:Symbol/Timeframe/AttributeGroup Bid,Ask/float32 variable
\load TEST2/1Min/TICK bin/ticks-example-2.csv bin/ticks-example-2.yaml
\o test_ticks.csv
\show TEST2/1Min/TICK 1970-01-01
EOF
if [ $? -ne 0 ]; then exit 1; fi

diff -q bin/ticks-example-2-output.csv test_ticks.csv && echo "Passed"
if [ $? -ne 0 ]; then exit 1; fi

rm -f test_ticks.csv
if [ $? -ne 0 ]; then exit 1; fi

marketstore connect -d `pwd`/testdata/mktsdb <<- EOF
\load TEST2/1Min/TICK bin/ticks-example-1.csv bin/ticks-example-1.yaml
\o test_ticks.csv
\show TEST2/1Min/TICK 1970-01-01
EOF
if [ $? -ne 0 ]; then exit 1; fi

cat bin/ticks-example-1-output.csv bin/ticks-example-2-output.csv > tmp.csv
diff -q tmp.csv test_ticks.csv && echo "Passed"
if [ $? -ne 0 ]; then exit 1; fi

rm -rf testdata test_ticks.csv tmp.csv
