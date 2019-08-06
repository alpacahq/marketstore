#!/bin/bash

echo $POLYGON_API_KEY

sed -i "s|<polygon_api_key>|$POLYGON_API_KEY|g" mkts.yml
exec marketstore start --config mkts.yml