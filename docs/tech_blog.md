
はじめまして、[@akkie](https://twitter.com/akkie30) と申します。
普段は株式会社メルカリにてバックエンドエンジニアをやっていますが、Alpaca Japan株式会社でも業務委託という形でお手伝いをさせていただいています。

本記事では、Alpacaでゼロベースから設計されたデータベースサーバであるMarketstoreについて、その詳細を語っていきたいと思います。

# Marketstoreとは
MarketStoreはファイナンスにおける時系列データに特化したデータベースサーバです。膨大な財務データを扱う上で必要なスケーラビリティを兼ね備えており、アルゴリズムトレードにおけるバックテストやチャート作成、何年間にもわたる金融商品の価格変化をナノ秒レベルで取り扱うことが可能です。

すでにAlpacaで数年以上の実用実績があり、本番環境に耐えうる品質となっています。 もちろん、バグを発見したり、MarketStoreに興味がある方はぜひ[開発にご協力](https://github.com/alpacahq/marketstore) いただければと思います。

# Marketstoreの設計思想
## パフォーマンスとスケーラビリティ
例えばアメリカでは[2019年時点で約4000の企業が上場](https://www.theglobaleconomy.com/USA/Listed_companies/) しており、この数は上昇傾向にはありません。[日本の市場も約4000の企業が上場しています](https://www.theglobaleconomy.com/rankings/Listed_companies/) 。たとえば株式のデータを扱うことを考えたときに、Marketstoreはこうした数の少なくとも分単位、将来的には秒単位やサブ秒単位のデータを遅延することなく取り扱うことを目指しています。

そのために設計上で各種の最適化を施しており、その中からいくつか主要なものをご紹介したいと思います。

### ファイルフォーマット
ファイナンスデータを扱う際には、いくつかの特徴があります。
扱うデータは時系列のデータ、つまり時刻をインデックスとして持つこと。 データの読み込みは時間幅を指定してバルクで取得することが多いこと。またTickデータなどは、事前にデータが到着する頻度を予測することができないこと、などです。

そうしたデータを高速に読み書きするために、Marketstoreのファイルフォーマットは時刻から一意にファイル上のデータの位置を特定できるような構造をとっています。
Marketstoreのバケット(MySQLで言うところのテーブル)に初めての書き込みを行う際に、データが到着する最大頻度("タイムフレーム"と呼びます)と1レコードのデータサイズを受け取ることで1年分のデータを格納可能なファイルを生成することでこれを実現します。
例えば下記のようなローソク足のデータを考えてみましょう。

* バケット名
  * "AAPL/1Min/OHLC" (Appleの株価の例)
* タイムフレーム
  * 1Min(1分)
* レコード定義
  * Epoch(タイムスタンプ. 必須): 64bit整数
  * Open: 32bit浮動小数点数
  * High: 同上
  * Low: 同上
  * Close: 同上

このレコードへの書き込みを行うと、Marketstoreは1年分のデータを格納可能なサイズのデータファイルを確保します。

ファイルサイズは以下のようになります。

= 共通ヘッダサイズ + 1年分のレコード数 * 1レコードのサイズ

= 37024(byte) + 365(日)*24(時間/日)*60(分/時間) * (64(bit)+32(bit)+32(bit)+32(bit)+32(bit))

= 100,952,224(bit) = 12,619,028(byte) 

スキーマ定義を固定し、事前にファイルサイズを確保しておくことはALTERクエリのようなスキーマ定義の変更を諦めることになりますが、引き出したいデータのタイムスタンプがわかればファイル上のどの位置に該当のレコードが保存されているのかがわかるので、クエリの速度は上がります。


### pandas DataFrameによるデータの読み書き

データ処理、機械学習を行う方にとっては、データをnumpy arrayやpandasライブラリのDataFrameに代入してから操作することが最も一般的なのではないでしょうか。
Marketstoreの公式クライアントである[pymarketstore](https://github.com/alpacahq/pymarketstore)では、numpy arrayによる書き込み、pandas.DataFameによる読み込み結果の取得をサポートしています。

```python
import numpy as np
import pandas as pd
import pymarketstore as pymkts

client = pymkts.Client()
data = np.array([(pd.Timestamp('2017-01-01 00:00').value / 10**9, "Hello")], dtype=[('Epoch', 'i8'), ('Bid', 'U16')])

# 書き込み
client.write(data, 'TEST/1Sec/Tick', isvariablelength=True)

# 読み込み
param = pymkts.Params('TEST', '1Sec', 'Tick', limit=10)
reply = client.query(param)

# 読み込んだ結果をpandas.DataFrameとして得る
dataFrame = reply.first().df()
```

データ処理を行う方にとって、これは開発時間を短縮して生産性を高めることにつながるでしょう。

### Materialize化する
データをMarketstoreから読み込む際に、簡単な処理を行いたい場合があるでしょう。
MarketstoreではAggregator functionという機能を使って、クエリするデータに簡単な関数を適用してレスポンスを得ることが可能です。

例えば次の例を考えてみます。

* \["Open", "High", "Low", "Close"\]カラムがあるバケットから、2010/01/01〜2011/01/15における"High"カラムの最大値と"Low"カラムの最小値を取得したい

Marketstoreはこれを実行するとき、
* Highカラムの値をすべて読み込んでその中の最大値を計算してレスポンスに追加

* 次にLowカラムの値をすべて読み込んでその中の最小値を計算してレスポンスに追加

のようなクエリ実行プランを立てません。
そうではなく、データベースの各レコードを走査しながら、各レコードに対して都度最大値や最小値の処理を適用し、各時刻時点での計算結果をメモリに保持して次に渡していくプランをとります。
結果として、大量のデータを使用した関数を適用する際にもメモリに大量のデータを乗せる必要なく、高速に処理を行うことができます。

最大値、最小値、移動平均値、Tickデータからローソク足データへの変換など、こうしてMaterialize処理することでパフォーマンスが上がる計算が時系列データには多く存在します。

もちろん、Materializeすることがパフォーマンス上不利に働くような計算もあるでしょうが、その場合は生のデータをクエリし、クライアント側で処理すればよいと考えます。
pandasのDataFrameの形でクエリレスポンスを取得できるMarketstoreでは、DBに頼り切らず、クライアント側でデータ処理を行うことも容易です。

## Write Ahead Logによるデータの完全性
書き込み処理の実装においてデータのインテグリティ(完全性)を担保するために、MarketstoreではWAL(Write Ahead Logging、ログ先行書き込み)と呼ばれる機構を採用しています。PostgreSQLを使用している人にはおなじみの言葉かもしれません。書き込みリクエストを受けたときにまずWALファイルに書き込まれ、バックグラウンドで実際のデータファイルへの書き込みが行われます。

こうすることで、トランザクション時にはログに書き込むだけで書き込みAPIのレスポンスを返すことができます。
Marketstoreには小さな・多くの書き込みリクエストが届くので、WALログへの書き込みを複数のリクエストでまとめて行うような最適化も行っています。
詳細について知りたい方は、公式のデザインドキュメントをご参照ください。

## 拡張性
データを保存するデータベースがあったとしても、保存するデータを外部から取得したり加工したりするために別のアプリケーションを用意するのは面倒なものです。
Marketstoreには、同じプロセスでそうした取得や加工を容易に行えるようにするための拡張性を備えており、plugin機能と呼ばれています。

MarketStore pluginは次の2つの種類があります
- Background型: バックグラウンドで動き定期的にMarketstoreにデータの保存や加工を行う
- Trigger型: Marketstoreに新しいデータが到着した際に発火し、イベント処理を行うことができる

go言語で定義された単純なinterfaceを実装するだけで新しいpluginを開発することが可能です。詳細は[公式リポジトリのREADME](https://github.com/alpacahq/marketstore/blob/master/plugins/README.md) をご参照ください。

# こんなこともできるMarketstore
## ナノ秒の精度
## クエリ時のデータ整形
## 文字列の保存
実はMarketstoreには文字列を保存する機能もあります。
```python
import numpy as np, pandas as pd, pymarketstore as pymkts

client = pymkts.Client()

# Write string
data = np.array([(pd.Timestamp('2017-01-01 00:00').value / 10**9, "Hello")], dtype=[('Epoch', 'i8'), ('Bid', 'U16')])
client.write(data, 'TEST/1Sec/Tick', isvariablelength=True)

# Query string
import numpy as np, pandas as pd, pymarketstore as pymkts
client = pymkts.Client()
param = pymkts.Params('TEST', '1Sec', 'Tick', limit=10)
reply = client.query(param)
reply.first().df()
```

しかし前述のように、Marketstoreは１年分のデータを書き込み時に確保するので、大きな文字列用のカラムをレコードに作成するのはディスク容量を圧迫する可能性があります。
どうしても自由なバイナリを保存するためにBase64エンコードされた文字列を保存したい等の用途でのみ文字列カラムを使うことをおすすめします。

## レプリケーション
読み込み性能をスケールさせたいものの、書き込むデータを外部から取得するためにコストがかかるケースなどでは、書き込むMarketstoreインスタンスを1台にしてデータを他のMarketstoreインスタンスにレプリケーションさせたいことがあります。そうした用途のために、MarketstoreはgRPCストリームで実装されたデータのレプリケーションをサポートしています。
詳細は[公式README](https://github.com/alpacahq/marketstore#replication) を参照してください。

## SQL
