package handlers_test

import (
	"fmt"
	"io/ioutil"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/alpacahq/marketstore/v4/contrib/alpaca/handlers"
	"github.com/alpacahq/marketstore/v4/executor"
	"github.com/alpacahq/marketstore/v4/utils/test"
)

func setup(t *testing.T, testName string) (tearDown func()) {
	t.Helper()

	rootDir, _ := ioutil.TempDir("", fmt.Sprintf("handlers_test-%s", testName))
	_, _, _, err := executor.NewInstanceSetup(rootDir, nil, nil, 5,
		true, true, false, true) // WAL Bypass
	assert.Nil(t, err)

	return func() { test.CleanupDummyDataDir(rootDir) }
}

func getTestTrade() []byte {
	return []byte(`{"data":{"ev":"T","T":"SPY","i":117537207,"x":2,"p":283.63,"s":2,"t":1587407015152775000,"c":[14, 37, 41],"z":2}}`)
}

func getTestQuote() []byte {
	return []byte(`{"data":{"ev":"Q","T":"SPY","x":17,"p":283.35,"s":1,"X":17,"P":283.4,"S":1,"c":[1],"t":1587407015152775000}}`)
}

func getTestAggregate() []byte {
	return []byte(`{"data":{"ev":"AM","T":"SPY","v":48526,"av":9663586,"op":282.6,"vw":282.0362,"o":282.13,"c":281.94,"h":282.14,"l":281.86,"a":284.4963,"s":1587409020000,"e":1587409080000}}`)
}

func TestHandlers(t *testing.T) {
	tearDown := setup(t, "TestHandlers")
	defer tearDown()

	// trade
	{
		handlers.MessageHandler(getTestTrade())
	}
	// quote
	{
		handlers.MessageHandler(getTestQuote())
	}
	// aggregate
	{
		handlers.MessageHandler(getTestAggregate())
	}
}
