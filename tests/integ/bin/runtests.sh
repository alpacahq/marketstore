#!/bin/bash

function exit_if_failed()
{
    if [[ $1 -ne 0 ]];
    then
        echo "Failed"
        exit 1;
    fi
}

# init data
rm -rf tests/integ/testdata test_ticks.csv && mkdir -p tests/integ/testdata/mktsdb
if [ $? -ne 0 ]; then \
    echo "Failed: cannot delete testdata and make testdata/mktsdb directory"
    exit 1;
fi


# import ticks-example-1.csv/yaml to TEST/1Min/TICK and check if the output of show commands match ticks-example-1-output.csv
./marketstore connect -d `pwd`/tests/integ/testdata/mktsdb <<- EOF
\create TEST/24H/TICK:Symbol/Timeframe/AttributeGroup Bid,Ask/float32 variable
\getinfo TEST/24H/TICK
\load TEST/24H/TICK tests/integ/bin/ticks-example-1.csv tests/integ/bin/ticks-example-1.yaml
\o test_ticks.csv
\show TEST/24H/TICK 1970-01-01
EOF
exit_if_failed $?

diff -q tests/integ/bin/ticks-example-1-output-24H.csv test_ticks.csv && echo "Passed"
exit_if_failed $?

rm -f test_ticks.csv
exit_if_failed $?


# import ticks-example-2.csv/yaml to TEST2/1Min/TICK and check if the output of show commands match ticks-example-2-output.csv
./marketstore connect -d `pwd`/tests/integ/testdata/mktsdb <<- EOF
\create TEST2/1Min/TICK:Symbol/Timeframe/AttributeGroup Bid,Ask/float32 variable
\load TEST2/1Min/TICK tests/integ/bin/ticks-example-2.csv tests/integ/bin/ticks-example-2.yaml
\o test_ticks.csv
\show TEST2/1Min/TICK 1970-01-01
EOF
exit_if_failed $?

diff -q tests/integ/bin/ticks-example-2-output.csv test_ticks.csv && echo "Passed"
exit_if_failed $?

rm -f test_ticks.csv
exit_if_failed $?


# import ticks-example-1.csv/yaml to TEST/1Min/TICK
# and check if the output of show commands match ticks-example-1-output.csv + ticks-example-2-output.csv
./marketstore connect -d `pwd`/tests/integ/testdata/mktsdb <<- EOF
\load TEST2/1Min/TICK tests/integ/bin/ticks-example-1.csv tests/integ/bin/ticks-example-1.yaml
\o test_ticks.csv
\show TEST2/1Min/TICK 1970-01-01
EOF
exit_if_failed $?

cat tests/integ/bin/ticks-example-1-output.csv tests/integ/bin/ticks-example-2-output.csv > tmp.csv
diff -q tmp.csv test_ticks.csv && echo "Passed"
exit_if_failed $?

rm -f test_ticks.csv
exit_if_failed $?


# import ticks-example-not-sorted-by-time.csv/yaml to TEST/1Min/TICK
# and check if the output of show commands match ticks-example-not-sorted-by-time-output.csv
./marketstore connect -d `pwd`/tests/integ/testdata/mktsdb <<- EOF
\create TEST3/1Min/TICK:Symbol/Timeframe/AttributeGroup Bid,Ask/float32 variable
\getinfo TEST3/1Min/TICK
\load TEST3/1Min/TICK tests/integ/bin/ticks-example-not-sorted-by-time.csv tests/integ/bin/ticks-example-not-sorted-by-time.yaml
\o test_ticks.csv
\show TEST3/1Min/TICK 1970-01-01
EOF
exit_if_failed $?

diff -q tests/integ/bin/ticks-example-not-sorted-by-time-output.csv test_ticks.csv && echo "Passed"
exit_if_failed $?

# remove the temporary files
rm -rf testdata test_ticks.csv tmp.csv
