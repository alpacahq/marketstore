## Financial Analysis UDA: Candler - converts raw price data to Candles for technical analysis

This module aggregates time series data into candles using the marketstore UDA interface. 

### The interesting functions can be called from SQL

Example: 

```
$ mkts -rootDir /data/market-data/mktsdb/
» select candlecandler('12Min',Open,High,Low,Close,Avg::Volume,Sum::Volume) from `TSLA/1Min/OHLCV` where Epoch > '2017-01-01' limit 10;
              =============================  ==========  ==========  ==========  ==========  ==========  ==========
                                      Epoch  Open        High        Low         Close       Volume_SUM  Volume_AVG
              =============================  ==========  ==========  ==========  ==========  ==========  ==========
              2017-01-03 14:24:00 +0000 UTC  214.86      215.3       210.96      212.5378    342337      57056.168
              2017-01-03 14:36:00 +0000 UTC  212.555     216.143     211.4       215.82      501743      41811.918
              2017-01-03 14:48:00 +0000 UTC  215.82      218.39      215.39      217.775     520423      43368.582
              2017-01-03 15:00:00 +0000 UTC  217.82      220.33      217.56      220.255     667460      55621.668
              2017-01-03 15:12:00 +0000 UTC  220.29      220.29      218.77      219.65      255877      21323.084
              2017-01-03 15:24:00 +0000 UTC  219.63      220         218.9       219.59      210390      17532.5
              2017-01-03 15:36:00 +0000 UTC  219.55      219.87      219.0444    219.1       140869      11739.083
              2017-01-03 15:48:00 +0000 UTC  219.18      219.25      217.46      217.71      203272      16939.334
              2017-01-03 16:00:00 +0000 UTC  217.72      218.25      216.59      217.87      122625      10218.75
              2017-01-03 16:12:00 +0000 UTC  218.04      218.317     216.61      216.68      87799       7316.5835
              =============================  ==========  ==========  ==========  ==========  ==========  ==========
              Elapsed query time: 63.754 ms

```

Notice that the parameters of the candlecandler() function in some cases have a
notation like "AVG:Volume" - those are dynamic tags used by the function to take
a variable number of inputs which are used by the candlecandler().


```go

/*
	Accum() sends new data to the aggregate
*/
func (ca *CandleCandler) Accum(cols io.ColumnInterface) error {
	if cols.Len() == 0 {
		return fmt.Errorf("Empty input to Accum")
	}
	/*
		Get the input column for "Price"
	*/
	openCols := ca.ArgMap.GetMappedColumns(requiredColumns[0].Name)
	highCols := ca.ArgMap.GetMappedColumns(requiredColumns[1].Name)
	lowCols := ca.ArgMap.GetMappedColumns(requiredColumns[2].Name)
	closeCols := ca.ArgMap.GetMappedColumns(requiredColumns[3].Name)
	open, err := candler.GetAverageColumnFloat32(cols, openCols)
	if err != nil {
		return err
	}
	high, err := candler.GetAverageColumnFloat32(cols, highCols)
	if err != nil {
		return err
	}
```

