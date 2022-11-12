## INSERT Statements

```
INSERT INTO data_location select_statement;
```

#### Where `data_directory` is a sub-directory pointing to the `rootDirectory` of your configuration file
```sql
-- example
INSERT INTO `gdax_BTC-USD/1D/OHLCV` SELECT * FROM `binance_BTC-USDT/1D/OHLCV`;
```

### Aggregate functions supported:
- TICKCANDLER
