#!/bin/bash

marketstore connect -d /project/data/mktsdb <<EOF
\create AAPL/1D/OHLCV:Symbol/Timeframe/AttributeGroup Open,High,Low,Close/float64:Volume/int64 fixed
\load AAPL/1D/OHLCV /project/data/contrib/ice/data/aapl_raw.csv /project/data/contrib/ice/data/aapl_raw.yaml
EOF
