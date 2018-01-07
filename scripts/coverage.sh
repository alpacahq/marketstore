#!/bin/sh

go test -coverprofile data/coverage $1 && go tool cover -html=data/coverage
