package io

import (
	"fmt"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"

	"github.com/pkg/errors"

	"github.com/alpacahq/marketstore/v4/utils"
)

type TimeBucketKey struct {
	// Key is the appended form, suitable for exported usage
	// e.g. Key="AAPL/1Min/OHLC:Symbol/Timeframe/AttributeGroup"
	key string `msgpack:"key"`
	/*
		itemKey     string
		categoryKey string
	*/
}

const DefaultTimeBucketSchema = "Symbol/Timeframe/AttributeGroup"

func NewTimeBucketKey(itemKey string, categoryKey_opt ...string) (mk *TimeBucketKey) {
	var categoryKey string

	if len(categoryKey_opt) != 0 && categoryKey_opt[0] != "" {
		categoryKey = categoryKey_opt[0]
	} else {
		categoryKey = DefaultTimeBucketSchema
	}

	return &TimeBucketKey{fmt.Sprintf("%s:%s", itemKey, categoryKey)}
}

func NewTimeBucketKeyFromString(itemCategoryString string) (mk *TimeBucketKey) {
	splitKey := strings.Split(itemCategoryString, ":")
	if len(splitKey) < 2 {
		return NewTimeBucketKey(splitKey[0])
	}
	return NewTimeBucketKey(splitKey[0], splitKey[1])
}

// e.g. "/project/marketstore/data/AMZN/1Min/TICK/2017.bin" -> (AMZN/1Min/TICK/2017.bin), (AMZN), (1Min), (TICK), (2017).
var wkpRegex = regexp.MustCompile(`([^/]+)/([^/]+)/([^/]+)/([^/]+)\.bin$`)

// NewTimeBucketKeyFromWalKeyPath converts a string in walKeyPath format
// (e.g. "/project/marketstore/data/AMZN/1Min/TICK/2017.bin") to a TimeBucketKey and year.
func NewTimeBucketKeyFromWalKeyPath(walKeyPath string) (tbk *TimeBucketKey, year int, err error) {
	group := wkpRegex.FindStringSubmatch(walKeyPath)
	// group should be like {"AAPL/1Min/Tick/2020.bin","AAPL","1Min","Tick","2017"} (len:5, cap:5)
	if len(group) != 5 {
		return nil, 0, errors.New(fmt.Sprintf("failed to extract TBK info from WalKeyPath:%v", walKeyPath))
	}

	year, err = strconv.Atoi(group[4])
	if err != nil {
		return nil, 0, errors.New(fmt.Sprintf("failed to extract year from WalKeyPath:%s", group[3]))
	}

	return NewTimeBucketKey(fmt.Sprintf("%s/%s/%s", group[1], group[2], group[3])), year, nil
}

func (mk *TimeBucketKey) String() (stringKey string) {
	/*
		return mk.itemKey + ":" + mk.categoryKey
	*/
	return mk.key
}

func (mk *TimeBucketKey) GetCatKey() (catKey string) {
	/*
		return mk.categoryKey
	*/
	splitKey := strings.Split(mk.key, ":")
	return splitKey[1]
}

func (mk *TimeBucketKey) GetItemKey() (itemKey string) {
	/*
		return mk.itemKey
	*/
	splitKey := strings.Split(mk.key, ":")
	return splitKey[0]
}

func (mk *TimeBucketKey) GetCategories() (cats []string) {
	/*
		return strings.Split(mk.categoryKey, "/")
	*/
	return strings.Split(mk.GetCatKey(), "/")
}

func (mk *TimeBucketKey) GetItems() (items []string) {
	/*
		return strings.Split(mk.itemKey, "/")
	*/
	return strings.Split(mk.GetItemKey(), "/")
}

func (mk *TimeBucketKey) GetItemInCategory(catName string) (item string) {
	for i, name := range mk.GetCategories() {
		if name == catName {
			return mk.GetItems()[i]
		}
	}
	return ""
}

func (mk *TimeBucketKey) GetMultiItemInCategory(catName string) (items []string) {
	for i, name := range mk.GetCategories() {
		if name == catName {
			presplit := mk.GetItems()[i]
			items = strings.Split(presplit, ",")
			return items
		}
	}
	return nil
}

func (mk *TimeBucketKey) SetItemInCategory(catName, itemName string) {
	cats := mk.GetCategories()
	items := mk.GetItems()
	for i, cat := range cats {
		if cat == catName {
			items[i] = itemName
		}
	}

	/*
		mk.itemKey = ""
		for i, item := range items {
			mk.itemKey += item
			if i != 2 {
				mk.itemKey += "/"
			}
		}
		mk.Key = mk.String()
	*/
	itemKey := ""
	for i, item := range items {
		itemKey += item
		if i != 2 {
			itemKey += "/"
		}
	}
	mk.key = itemKey + ":" + mk.GetCatKey()
}

func (mk *TimeBucketKey) GetTimeFrame() (tf *utils.Timeframe, err error) {
	tfs := mk.GetItemInCategory("Timeframe")
	if len(tfs) == 0 {
		return &utils.Timeframe{}, fmt.Errorf("Error: Unable to get timeframe from key")
	}

	if tf = utils.TimeframeFromString(tfs); tf == nil {
		err = fmt.Errorf("error: Unable to get timeframe from key")
	}

	return
}

func (mk *TimeBucketKey) GetPathToYearFiles(rootDir string) string {
	return filepath.Join(rootDir, mk.GetItemKey())
}
