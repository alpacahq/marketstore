package io

import (
	"fmt"
	"path/filepath"
	"strings"

	"github.com/alpacahq/marketstore/utils"
)

type TimeBucketKey struct {
	Key string `msgpack:"key"` // Key is the appended form, suitable for exported usage
	/*
		itemKey     string
		categoryKey string
	*/
}

var (
	defaultTimeBucketSchema string
)

func init() {
	defaultTimeBucketSchema = "Symbol/Timeframe/AttributeGroup"
}

func NewTimeBucketKey(itemKey string, categoryKey_opt ...string) (mk *TimeBucketKey) {
	var categoryKey string
	if len(categoryKey_opt) != 0 {
		categoryKey = categoryKey_opt[0]
	} else {
		categoryKey = defaultTimeBucketSchema
	}
	mk = new(TimeBucketKey)
	mk.Key = itemKey + ":" + categoryKey
	return mk
}

func NewTimeBucketKeyFromString(itemCategoryString string) (mk *TimeBucketKey, err error) {
	splitKey := strings.Split(itemCategoryString, ":")
	if len(splitKey) != 2 {
		return nil,
			fmt.Errorf("TimeBucketKey string should be of form \"itemKey:catKey\" - have %s",
				itemCategoryString)
	}
	return NewTimeBucketKey(splitKey[0], splitKey[1]), nil
}

func (mk *TimeBucketKey) String() (stringKey string) {
	/*
		return mk.itemKey + ":" + mk.categoryKey
	*/
	return mk.Key
}
func (mk *TimeBucketKey) GetCatKey() (catKey string) {
	/*
		return mk.categoryKey
	*/
	splitKey := strings.Split(mk.Key, ":")
	return splitKey[1]
}
func (mk *TimeBucketKey) GetItemKey() (itemKey string) {
	/*
		return mk.itemKey
	*/
	splitKey := strings.Split(mk.Key, ":")
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

func (mk *TimeBucketKey) SetItemInCategory(catName string, itemName string) {
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
	mk.Key = itemKey + ":" + mk.GetCatKey()
}

func (mk *TimeBucketKey) GetTimeFrame() (tf *utils.Timeframe, err error) {
	tfs := mk.GetItemInCategory("Timeframe")
	if len(tfs) == 0 {
		return &utils.Timeframe{}, fmt.Errorf("Error: Unable to get timeframe from key")
	}
	return utils.TimeframeFromString(tfs), nil
}

func (mk *TimeBucketKey) GetPathToYearFiles(rootDir string) string {
	return filepath.Join(rootDir, mk.GetItemKey())
}
