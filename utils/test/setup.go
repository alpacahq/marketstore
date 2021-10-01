package test

import (
	"fmt"
	"io"
	"os"
	"path"
	"strconv"
	"strings"
	"time"

	"github.com/alpacahq/marketstore/v4/utils"
	. "github.com/alpacahq/marketstore/v4/utils/io"
	"github.com/alpacahq/marketstore/v4/utils/log"
)

func checkfail(err error, msg string) {
	if err != nil {
		log.Error("Message: %v - Error: %v", msg, err)
		os.Exit(1)
	}
}

var tfIntervals = map[string]int{
	"1Min":  24 * 60,
	"5Min":  24 * 12,
	"15Min": 24 * 4,
	"1H":    24,
	"4H":    6,
	"1D":    1,
}

func makeCatFile(dir string, catname string) {
	file, err := os.OpenFile(path.Join(dir, "category_name"), os.O_CREATE|os.O_RDWR,
		0777)
	checkfail(err, "makeCatFile: Unable to open category file for writing ")
	file.Write([]byte(catname))
}
func makeRootDir(root string) {
	err := os.Mkdir(root, 0777)
	if !os.IsExist(err) {
		checkfail(err, "makeRootDir: Unable to create directory: "+root)
	}
}
func makeCatDir(root string, catname string, items []string) {
	base := root + "/"
	makeCatFile(base, catname)
	for _, name := range items {
		err := os.Mkdir(base+name, 0777)
		if !os.IsExist(err) {
			checkfail(err, "makeCatDir: Unable to create directory: "+name)
		}
	}
}
func makeFakeFileInfoCurrency(year string, filePath string, timeFrame string) *TimeBucketInfo {
	tf := utils.TimeframeFromString(timeFrame)
	yr, _ := strconv.Atoi(year)
	dsv := NewDataShapeVector(
		[]string{"Open", "High", "Low", "Close"},
		[]EnumElementType{FLOAT32, FLOAT32, FLOAT32, FLOAT32},
	)
	return NewTimeBucketInfo(*tf, filePath, "Fake fileinfo", int16(yr), dsv, FIXED)
}

func makeFakeFileInfoStock(year string, filePath string, timeFrame string) *TimeBucketInfo {
	tf := utils.TimeframeFromString(timeFrame)
	yr, _ := strconv.Atoi(year)
	dsv := NewDataShapeVector(
		[]string{"Open", "High", "Low", "Close", "Volume"},
		[]EnumElementType{FLOAT32, FLOAT32, FLOAT32, FLOAT32, INT32},
	)
	return NewTimeBucketInfo(*tf, filePath, "Fake fileinfo", int16(yr), dsv, FIXED)
}

func makeYearFiles(root string, years []string, withdata bool, withGaps bool, tf string, itemsWritten *map[string]int, isStock bool) {
	base := root + "/"
	makeCatFile(base, "Year")
	for _, year := range years {
		filename := base + year + ".bin"
		file, err := os.OpenFile(filename, os.O_CREATE|os.O_RDWR, 0777)
		checkfail(err, "Unable to create file: "+filename)
		var f *TimeBucketInfo
		if isStock {
			f = makeFakeFileInfoStock(year, filename, tf)
		} else {
			f = makeFakeFileInfoCurrency(year, filename, tf)
		}
		err = WriteHeader(file, f)
		checkfail(err, "Unable to write header to: "+filename)
		if withdata {
			(*itemsWritten)[filename], err = WriteDummyData(file, year, tf, withGaps, isStock)
			checkfail(err, "Unable to write dummy data")
			//			fmt.Printf("File: %s Number: %d\n", filename, (*itemsWritten)[filename])
		}
	}
}

func ParseT(s string) time.Time {
	t, _ := time.Parse("2006-01-02 15:04:05", s)
	return t
}

type candle struct {
	t          time.Time
	o, h, l, c float32
}

func parseCandleText(text string) []*candle {
	candles := []*candle{}
	for _, line := range strings.Split(text, "\n") {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		items := strings.Split(line, "\t")
		open, _ := strconv.ParseFloat(items[1], 32)
		high, _ := strconv.ParseFloat(items[2], 32)
		low, _ := strconv.ParseFloat(items[3], 32)
		close, _ := strconv.ParseFloat(items[4], 32)

		timestamp := ParseT(items[0])
		candle := &candle{
			t: timestamp,
			o: float32(open),
			h: float32(high),
			l: float32(low),
			c: float32(close),
		}
		candles = append(candles, candle)
	}

	return candles
}

func DummyDataFromText(rootDir, symbol, timeframe, data string) {
	attrName := "OHLC"
	makeRootDir(rootDir)
	makeCatDir(rootDir, "Symbol", []string{symbol})
	symbase := path.Join(rootDir, symbol)
	makeCatDir(symbase, "Timeframe", []string{timeframe})
	tfbase := path.Join(symbase, timeframe)
	makeCatDir(tfbase, "AttributeGroup", []string{attrName})
	attbase := path.Join(tfbase, attrName)
	makeCatFile(attbase, "Year")

	candles := parseCandleText(data)
	yearCandles := map[int][]*candle{}
	for _, candle := range candles {
		y := candle.t.Year()
		yearCandles[y] = append(yearCandles[y], candle)
	}

	for y, candles := range yearCandles {
		year := fmt.Sprintf("%d", y)
		fileName := attbase + "/" + year + ".bin"
		file, err := os.OpenFile(fileName, os.O_CREATE|os.O_RDWR, 0644)
		checkfail(err, "Unable to create file: "+fileName)
		fileInfo := makeFakeFileInfoCurrency(year, fileName, timeframe)
		err = WriteHeader(file, fileInfo)
		checkfail(err, "Unable to write header to: "+fileName)

		for _, candle := range candles {
			offset := TimeToOffset(candle.t, fileInfo.GetTimeframe(), fileInfo.GetRecordLength())
			ondisk := ohlc{
				index: TimeToIndex(candle.t, fileInfo.GetTimeframe()),
				o:     candle.o,
				h:     candle.h,
				l:     candle.l,
				c:     candle.c,
			}
			data := SwapSliceData([]ohlc{ondisk}, byte(0)).([]byte)
			file.Seek(offset, io.SeekStart)
			_, err = file.Write(data)
			checkfail(err, "Unable to write data to: "+fileName)
		}
	}
}

type ohlc struct {
	index      int64
	o, h, l, c float32
}

type ohlcv struct {
	index      int64
	o, h, l, c float32
	v          int32
}

func WriteDummyData(f *os.File, year, tf string, makeGap, isStock bool) (int, error) {
	var yr int
	n, err := fmt.Sscanf(year, "%d", &yr)
	if n != 1 || err != nil {
		return 0, fmt.Errorf("failed to convert string year=%s to int: %w", year, err)
	}
	var candlesCurrency []ohlc
	var candlesStock []ohlcv
	endTime := time.Date(yr, time.December, 31, 23, 59, 59, 0, time.UTC)
	numDays := endTime.YearDay()
	var gap, index, ind int64
	var o, h, l, c float32
	var v int32
	var numberNotEmpty int
	//fmt.Printf("Year: %s Yr: %d NDays: %d Intervals: %d\n",year, yr, numDays, numDays*tfIntervals[tf])
	index = 1 // First interval is 1
	for i := 0; i < numDays*tfIntervals[tf]; i++ {
		fi := float32(1 + i%15)
		o = 0.1 * fi
		h = 0.2 * fi
		l = 0.3 * fi
		c = 0.4 * fi
		v = int32(i + 1)
		if (gap == 0) && (i%100 == 0) && makeGap {
			//			fmt.Printf("\n")
			gap = 99
		}
		if gap != 0 {
			ind = 0
			gap--
		} else {
			ind = index
		}
		if ind != 0 {
			numberNotEmpty++
		}
		if isStock {
			candlesStock = append(candlesStock, ohlcv{ind, o, h, l, c, v})
		} else {
			candlesCurrency = append(candlesCurrency, ohlc{ind, o, h, l, c})
		}
		//		fmt.Printf(":%d:",ind)
		index += 1
	}

	var buffer []byte
	if isStock {
		buffer = SwapSliceData(candlesStock, byte(0)).([]byte)
	} else {
		buffer = SwapSliceData(candlesCurrency, byte(0)).([]byte)
	}
	_, err = f.Write(buffer)
	//fmt.Printf("num: %d\n",numberNotEmpty)
	return numberNotEmpty, err
}

func MakeDummyCurrencyDir(root string, withdata bool, withGaps bool) map[string]int {
	itemsWritten := make(map[string]int, 0)
	makeRootDir(root)
	symbols := []string{"EURUSD", "USDJPY", "NZDUSD"}
	timeframes := []string{"1Min", "5Min", "15Min", "1H", "4H", "1D"}
	attgroups := []string{"OHLC"}
	years := []string{"2000", "2001", "2002"}
	makeCatDir(root, "Symbol", symbols)
	var symbase string
	var tfbase string
	var attbase string
	for _, sym := range symbols {
		symbase = root + "/" + sym
		makeCatDir(symbase, "Timeframe", timeframes)
		for _, tf := range timeframes {
			tfbase = symbase + "/" + tf
			makeCatDir(tfbase, "AttributeGroup", attgroups)
			for _, attname := range attgroups {
				attbase = tfbase + "/" + attname
				makeYearFiles(attbase, years, withdata, withGaps, tf, &itemsWritten, false)
			}
		}
	}
	return itemsWritten
}

func MakeDummyStockDir(root string, withdata bool, withGaps bool) map[string]int {
	itemsWritten := make(map[string]int, 0)
	makeRootDir(root)
	symbols := []string{"AAPL", "BBPL", "CCPL"}
	timeframes := []string{"1Min", "5Min", "15Min", "1H", "4H", "1D"}
	attgroups := []string{"OHLCV"}
	years := []string{"2000", "2001", "2002"}
	makeCatDir(root, "Symbol", symbols)
	var symbase string
	var tfbase string
	var attbase string
	for _, sym := range symbols {
		symbase = root + "/" + sym
		makeCatDir(symbase, "Timeframe", timeframes)
		for _, tf := range timeframes {
			tfbase = symbase + "/" + tf
			makeCatDir(tfbase, "AttributeGroup", attgroups)
			for _, attname := range attgroups {
				attbase = tfbase + "/" + attname
				makeYearFiles(attbase, years, withdata, withGaps, tf, &itemsWritten, true)
			}
		}
	}
	return itemsWritten
}

func CleanupDummyDataDir(root string) {
	err := os.RemoveAll(root)
	if err != nil {
		log.Error("Failed to clean up dummy data directory - Error: %v", err)
	}
}
