#!/bin/bash

# This line queries a server on localhost at port 5993 using the Query RPC
curl -s --data-raw '{"jsonrpc":"2.0", "method":"DataService.Query", "id":1, "params": {"Requests": [{"destination":"AAPL/1Min/OHLCV"}]}}' -H 'Content-Type: application/json' http://localhost:5993/rpc

# This line retrieves the current list of symbols via the ListSymbols RPC
curl -s --data-raw '{"jsonrpc":"2.0", "method":"DataService.ListSymbols", "id":1, "params": {"parameters": {}}}' -H 'Content-Type: application/json' http://localhost:5993/rpc
