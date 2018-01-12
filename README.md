## Install

### Install Golang
If you are running macOS and have brew installed, run the following and skip to **Install Glide**:

```shell
$ brew install golang
```

To install on Linux, download the most recent archive from [golang.org](https://golang.org/dl/), then extract to `/usr/local`.

```shell
$ sudo tar -C /usr/local -xzf go$VERSION.$OS-$ARCH.tar.gz
```

Then add `/usr/local/go/bin` to the PATH environment variable by adding the following to the bottom
of your `~/.profile`.

```shell
export PATH=$PATH:/usr/local/go/bin
export GOPATH=$HOME
```

Lastly, source your `~/.profile`.

```shell
$ source ~/.profile
```

Test your installation with `go version`.

### Install Glide (Golang's Vendor Package Management)

You need [glide](http://glide.sh/) to download dependencies.  If you have installed brew, simply

```shell
$ brew install glide
```

Otherwise, run the following.

```shell
$ go get github.com/Masterminds/glide
$ go install github.com/Masterminds/glide
```

You will find glide in `$GOPATH/bin`.

### Build and Install marketstore

`make configure` will download dependencies in `src/vendor`.

`make unittest` will confirm a working installation.

`make` will install the marketstore executables to `$GOPATH/bin`.

```shell
make configure
make unittest
make
```

To verify, the output below should match the last few lines of
your output for ```make unittest```.

```
ok      github.com/alpacahq/marketstore/utils   1.006s  coverage: 17.4% of statements
ok      github.com/alpacahq/marketstore/utils/functions 0.004s  coverage: 61.9% of statements
ok      github.com/alpacahq/marketstore/utils/io        0.006s  coverage: 31.3% of statements
?       github.com/alpacahq/marketstore/utils/log       [no test files]
ok      github.com/alpacahq/marketstore/utils/rpc/msgpack2      0.004s  coverage: 83.3% of statements
?       github.com/alpacahq/marketstore/utils/stats     [no test files]
?       github.com/alpacahq/marketstore/utils/test      [no test files]
ok      github.com/alpacahq/marketstore/feedmanager     4.083s  coverage: 85.1% of statements
?       github.com/alpacahq/marketstore/feedmanager/testplugin  [no test files]
```

### Test an Example

Let's test out marketstore by running the ```runtest.sh``` example under ```cmd/tools/mkts/examples```.

This test script will create a bucket, load example tick data into the bucket, and run a simple query.

The last few lines of output should match the following:

```
=============================  ==========  ==========  ==========  
                        Epoch  Bid         Ask         Nanoseconds  
=============================  ==========  ==========  ==========  
2016-12-31 02:37:57 +0000 UTC  1.05185     1.05197     139999810   
2016-12-31 02:38:02 +0000 UTC  1.05185     1.05198     389999832   
2016-12-31 02:38:09 +0000 UTC  1.05188     1.052       389999583   
2016-12-31 02:38:09 +0000 UTC  1.05189     1.05201     889999385   
2016-12-31 02:38:10 +0000 UTC  1.05186     1.05197     139999706   
2016-12-31 02:38:10 +0000 UTC  1.05186     1.05192     389999188   
2016-12-31 02:38:10 +0000 UTC  1.05181     1.05189     639999508   
2016-12-31 02:38:10 +0000 UTC  1.05182     1.0519      889999829   
2016-12-31 02:38:11 +0000 UTC  1.05181     1.05189     389999631   
2016-12-31 02:38:18 +0000 UTC  1.0518      1.0519      139999900   
=============================  ==========  ==========  ==========  
Elapsed parse time: 19.523 ms
Elapsed query time: 4.707 ms
```

## Configuration

In order to run MarketStore, a configuration .yaml file is needed. A default file is included in the codebase
above and is called mkts_config.yaml. This path to this file is passed in to the launcher binary with the
'-config' flag, or by default it finds a file with that name in the directory it is running from. This file
should look as follows:

```shell
root_directory: /project/data/mktsdb
listen_port: 5993
log_level: info
queryable: true
stop_grace_period: 0
wal_rotate_interval: 5
stale_threshold: 5
enable_add: true
enable_remove: false

```

* __root_directory__: allows the user to specify the directory in which the MarketStore database resides (string)
* __listen_port__: specifies the port that MarketStore will serve through (integer)
* __timezone__: system timezone by name of TZ database (e.g. America/New_York) default=UTC
* __log_level__: allows the user to specify the log level (string: info, warning, error)
* __queryable__: allows the user to run MarketStore in polling-only mode, where it will not respond to query (bool)
* __stop_grace_period__: sets the amount of time MarketStore will wait to shutdown after a SIGINT signal is received (integer: seconds)
* __wal_rotate_interval__: frequency at which the WAL file will be trimmed after being flushed to disk (integer, minutes)
* __stale_threshold__: threshold by which MarketStore will declare a symbol stale (integer, days)
* __enable_add__: flag allowing new symbols to be added to DB via /write API
* __enable_remove__: flag allowing symbols to be removed from DB via /write API
* __triggers__: list of trigger plugins
* __bgworkers__: list of background worker plugins

## Update dependency

If you need update the code version in `src/vendor`, run make update.  `src/glide.lock` will be updated.
If you need to add more dependencies, update `src/glide.yaml` and `make update`

## Release

The release procedure for MarketStore is as follows:

    - git flow release start <version>

Note that the version will take the form of: 1.0.0, but the tag will look like: v1.0.0

At this point, no further commits will be made except for bug fixes found during testing.
If changes are made, make them in this branch, and merge them back into the main release branch.

    - git flow release finish <version>
    - git push origin v<version>
    - git push origin master
    - git push origin develop

Upon completion, CircleCI will build a docker container for the release, with the name:

    alpacahq/marketstore:v<version>

The marketstore container can then be pulled down from dockerhub, and run using the makefile located
in dockerfiles/marketstore in the trading repo.

In case a hotfix is needed:

    - git flow hotfix start <next version>
    - git cherry-pick <commit>
    - git flow hotfix finish <version>
    - git push origin v<version>
    - git push origin master
