# MarketStore
## Introduction
MarketStoreはファイナンスにおける時系列データに特化したデータベースサーバです。
スケーラビリティについて十分に考慮されており、あなたのシステムのあらゆるところからアクセス可能で、かつ拡張可能なDataFrameサービスとして使用することができます。


膨大な金融市場データの扱いにおいて問題となるスケーラビリティを確保するために、MarketStoreはゼロベースから設計されています。アルゴリズム取引におけるバックテストやチャート作成, 何年間にもわたるデータからなる価格履歴をTick(株式、債権、為替などの金融商品における価格変化の最小単位)レベルの粒度で扱うことができます。
米国株式や暗号通貨の分野を主な対象として扱っています。

もしあなたが大量のHDF5ファイルの扱いに苦しんでいるのであれば、MarketStoreはその完璧な解決策となるでしょう。

インストールしていただくことで入る機能に加えて、GDAXから取得した暗号通貨の価格データを用いてDBに書き込みを行うなど、プラグイン設定も簡単に使うことができます。

MarketStoreを使うことで、ネットワーク越しであってもローカルディスクにあるHDF5ファイルに対して行うのと同じくらい低いレイテンシでDataFrameに対してクエリをかけることができます。新規データの書き込みであれば、DataFrameの100倍以上の速度が見込めます。これはストレージの形式を特定のデータのタイプやユースケースに最適化しており、また最近のファイルシステム/ハードウェアの特性を考慮した上でMarketStoreが設計されているためです。

MarketStoreは本番環境で使っていただける品質になっています。すでにAlpacaでは重要なビジネス向けに何年も使用を重ねてきています。
もしバグを発見したり、MarketStoreに興味がある方は開発にご協力いただければと思います。


## Install

### Docker
すぐにMarketStoreを使ってみたい場合は、最新の [docker image](https://hub.docker.com/r/alpacamarkets/marketstore/tags/) を使ってDBインスタンスを生成する方法をおすすめします。 デフォルトの mkts.yml (設定ファイル)もあり、 `/data` をデータ保存先のルートディレクトリとして定義された状態でコンテナを起動できます。 デフォルト設定でコンテナを立ち上げる場合は以下のコマンドを実行してください。
``` sh
docker run -i -p 5993:5993 alpacamarkets/marketstore:latest
```

自分でカスタマイズした設定ファイル `mkts.yml` でインスタンスを起動したい場合は、 
下記のような手順で自分で新しいコンテナを作って、その中に設定ファイルを埋め込んでください。
``` sh
docker create --name mktsdb -p 5993:5993 alpacamarkets/marketstore:latest
docker cp mkts.yml mktsdb:/etc/mkts.yml
docker start -i mktsdb
```

起動中のDockerコンテナに対してセッションを張る場合は以下のようにしてください。
``` sh
marketstore connect --url localhost:5993
```

### Source
MarketStoreはGo (一部CGO)を用いて実装されているので、ソースコードからビルドすることも簡単です。Go `1.11` 以上のバージョンを使用し、また依存管理には`go mod` を使用しています。
``` sh
go get -u github.com/alpacahq/marketstore
```
依存を解決するためには以下をリポジトリのディレクトリ内で実行します。
``` sh
make vendor
```
以下でバイナリをコンパイル＆インストールしてください。
``` sh
make install
```
必須ではありませんが、本リポジトリに付属しているプラグインをインストールすることも簡単にできます。
``` sh
make plugins
```

## Usage
`marketstore` コマンドで、使用することができるコマンド一覧が得られます。
```
marketstore
```
$GOPATHの設定によってはこちら
```
$GOPATH/bin/marketstore
```

`mkts.yml` という名前で設定ファイルを独自に定義することができますが、デフォルトの設定ファイルであればコマンドで生成することもできます。 
```
$GOPATH/bin/marketstore init
```
MarketStoreを起動します。
```
$GOPATH/bin/marketstore start
```

以下のような出力が得られたら起動成功です。
```
example@alpaca:~/go/bin/src/github.com/alpacahq/marketstore$ marketstore
I0619 16:29:30.102101    7835 log.go:14] Disabling "enable_last_known" feature until it is fixed...
I0619 16:29:30.102980    7835 log.go:14] Initializing MarketStore...
I0619 16:29:30.103092    7835 log.go:14] WAL Setup: initCatalog true, initWALCache true, backgroundSync true, WALBypass false:
I0619 16:29:30.103179    7835 log.go:14] Root Directory: /example/go/bin/src/github.com/alpacahq/marketstore/project/data/mktsdb
I0619 16:29:30.144461    7835 log.go:14] My WALFILE: WALFile.1529450970104303654.walfile
I0619 16:29:30.144486    7835 log.go:14] Found a WALFILE: WALFile.1529450306968096708.walfile, entering replay...
I0619 16:29:30.244778    7835 log.go:14] Beginning WAL Replay
I0619 16:29:30.244861    7835 log.go:14] Partial Read
I0619 16:29:30.244882    7835 log.go:14] Entering replay of TGData
I0619 16:29:30.244903    7835 log.go:14] Replay of WAL file /example/go/bin/src/github.com/alpacahq/marketstore/project/data/mktsdb/WALFile.1529450306968096708.walfile finished
I0619 16:29:30.289401    7835 log.go:14] Finished replay of TGData
I0619 16:29:30.340760    7835 log.go:14] Launching rpc data server...
I0619 16:29:30.340792    7835 log.go:14] Initializing websocket...
I0619 16:29:30.340814    7835 plugins.go:14] InitializeTriggers
I0619 16:29:30.340824    7835 plugins.go:42] InitializeBgWorkers
```

## Configuration
MarketStoreを実行するにはYAMLで書かれた設定ファイル `mkts.yml` が必要になります。`marketstore init` コマンドでデフォルトを生成できます。設定ファイルのパスは `marketstore start --config [設定ファイルへのパス]` という形で `--config` オプションで指定してください。指定されなかった場合は、marketstoreが実行されたパス内から`mkts.yml`を探します。

### 設定項目
Var | Type | Description
--- | --- | ---
root_directory | string |  MarketStore データベースが使用するディレクトリ
listen_port | int | MarketStoreが使用するポート番号
timezone | string |  タイムゾーン. `TZ` に定義されている値 (例 America/New_York)
log_level | string  | 出力する最低ログレベル `(info | warning | error)`
queryable | bool | polling-onlyモードで起動する場合はfalseにします。その場合はqueryに応答しなくなります。
stop_grace_period | int | SIGINT シグナルを受信してから終了するまでに待つ時間
wal_rotate_interval | int | ディスクにフラッシュしてWALファイルがトリムされる頻度[分]
stale_threshold | int | MarketStoreがシンボルを古いものと認識するしきい値[日]
enable_add | bool | `/write` APIを通じてシンボルを新規作成することを許可するかどうか
enable_remove | bool | `/write/ APIを通じてシンボルを削除することを許可するかどうか  
triggers | slice | triggerプラグインのリスト
bgworkers | slice | background workerプラグインのリスト

### デフォルト設定(mkts.yml)
```
root_directory: data
listen_port: 5993
log_level: info
queryable: true
stop_grace_period: 0
wal_rotate_interval: 5
stale_threshold: 5
enable_add: true
enable_remove: false
```


## Clients
MarketStoreインスタンスが起動したあとは、Tickデータを読み書きし始めることができます。

### Python
[pymarketstore](https://github.com/alpacahq/pymarketstore) が標準のPythonクライアントになっています。すでにMarketStoreが起動していることを確認の上、ご使用ください。

```
In [1]: import pymarketstore as pymkts

## query (データの参照)

In [2]: param = pymkts.Params('BTC', '1Min', 'OHLCV', limit=10)

In [3]: cli = pymkts.Client()

In [4]: reply = cli.query(param)

In [5]: reply.first().df()
Out[5]:
                               Open      High       Low     Close     Volume
Epoch
2018-01-17 17:19:00+00:00  10400.00  10400.25  10315.00  10337.25   7.772154
2018-01-17 17:20:00+00:00  10328.22  10359.00  10328.22  10337.00  14.206040
2018-01-17 17:21:00+00:00  10337.01  10337.01  10180.01  10192.15   7.906481
2018-01-17 17:22:00+00:00  10199.99  10200.00  10129.88  10160.08  28.119562
2018-01-17 17:23:00+00:00  10140.01  10161.00  10115.00  10115.01  11.283704
2018-01-17 17:24:00+00:00  10115.00  10194.99  10102.35  10194.99  10.617131
2018-01-17 17:25:00+00:00  10194.99  10240.00  10194.98  10220.00   8.586766
2018-01-17 17:26:00+00:00  10210.02  10210.02  10101.00  10138.00   6.616969
2018-01-17 17:27:00+00:00  10137.99  10138.00  10108.76  10124.94   9.962978
2018-01-17 17:28:00+00:00  10124.95  10142.39  10124.94  10142.39   2.262249

## write (データの書き込み)

In [7]: import numpy as np

In [8]: import pandas as pd

In [9]: data = np.array([(pd.Timestamp('2017-01-01 00:00').value / 10**9, 10.0)], dtype=[('Epoch', 'i8'), ('Ask', 'f4')])

In [10]: cli.write(data, 'TEST/1Min/Tick')
Out[10]: {'responses': None}

In [11]: cli.query(pymkts.Params('TEST', '1Min', 'Tick')).first().df()
Out[11]:
                            Ask
Epoch
2017-01-01 00:00:00+00:00  10.0

```

### Command-line
marketstoreインスタンスに接続するコマンドはこちらです。
```
// For a local db-
marketstore connect --dir <path>
// For a server-
marketstore connect --url <address>
```
接続後はsqlセッションを通じてコマンドを実行することができます。

## Plugins
Goにおけるプラグインは Go 1.10以上(Linux)で最適に動きます。プラグインについての詳細は [plugins package](./contrib/plugins/) をご覧ください。
ここでは特に取り上げたいいくつかのプラグインについてご紹介します。

### Streaming
WebSocketのストリームを利用してリアルタイムでアップデートを受け取ることができます。
`/ws` でWebSocket接続を受けつけることができ、データをPushすることもできます。
詳細については [the package](./contrib/stream/) をご参照ください。

### GDAX Data Feeder
[GDAX](https://docs.gdax.com/#get-historic-rates) から暗号通貨のデータを取得することができます。
MarketStoreをインストールすると、すぐにDataFrame型の情報をqueryして取得することができます。ネットワーク越しであっても、ローカルディスク上のHDF5ファイルを読み込むのと同じくらい低いレイテンシで扱うことができますし、最後尾にデータを追加する処理はDataFrameで行う100倍も速いはずです。MarketStoreはストレージの形式を特定のデータ型、ユースケース、またファイルシステムやハードウェアの特性に最適化した形で設計されているためです。

data pollerを設定してGDAXからデータを取得しはじめましょう。
詳細については [the package](./contrib/gdaxfeeder/) をご参照ください。

### On-Disk Aggregation
このプラグインを使うと、 tick/分 レベルのデータだけを気にすればよくなります。時系列ベースでディスク上のデータのアグリゲーションをしてくれるプラグインです。詳しくは
[the package](./contrib/ondiskagg/)をご参照ください。


## 開発に協力していただける方へ
興味がある方はぜひMarketStoreの開発にご協力ください！
GithubのIssue報告やPullRequestでも構いませんし、`oss@alpaca.markets` に直接ご連絡いただいても構いません。 
Pull Requestを作る際は、下記のコマンドを実行してテストが通る状態であることをご確認ください。

``` sh
make unittest
```

### プラグイン開発
我々の取り組む分野に対する要求は多岐に渡るため、MarketStoreは柔軟にプラグインを追加できるアーキテクチャをとっています。
独自のプラグインを生成したい方は [plugins](./plugins/) をご覧ください。
