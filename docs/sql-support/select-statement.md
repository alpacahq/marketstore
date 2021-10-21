## SELECT Statements

```
SELECT [ ALL | DISTINCT ] select_expr [, ...]
[ FROM data_directory [, ...] ]
[ WHERE condition ]
[ { LIMIT [ count ] } ]
```

#### Where `data_directory` is a sub-directory pointing to the `rootDirectory` of your configuration file

``` sql
-- example
SELECT * FROM `gdax_BTC-USD/1D/OHLCV`; --escape dashes by wrapping it with backticks
```
