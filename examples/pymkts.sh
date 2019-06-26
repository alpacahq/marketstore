#!/bin/bash

docker run -it alpacamarkets/pymkts <<EOF
querydf('AAPL', host='mktsdb-iex-1').head()
EOF
