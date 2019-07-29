#!/bin/bash

docker run -i alpacamarkets/pymkts <<EOF
querydf('AAPL', host='mktsdb-iex-1').head()
EOF
