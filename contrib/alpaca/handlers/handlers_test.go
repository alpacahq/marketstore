package handlers_test

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/alpacahq/marketstore/v4/contrib/alpaca/handlers"
	"github.com/alpacahq/marketstore/v4/executor"
)

func setup(t *testing.T) {
	t.Helper()

	rootDir := t.TempDir()
	_, _, _, err := executor.NewInstanceSetup(rootDir, nil, nil, 5,
		executor.BackgroundSync(false), executor.WALBypass(true)) // WAL Bypass
	assert.Nil(t, err)
}

func getTestTrade() []byte {
	const data = `{"data":{"ev":"T","T":"SPY","i":117537207,"x":2,"p":283.63,"s":2,"t":1587407015152775000,` +
		`"c":[14, 37, 41],"z":2}}`
	return []byte(data)
}

func getTestQuote() []byte {
	const data = `{"data":{"ev":"Q","T":"SPY","x":17,"p":283.35,"s":1,"X":17,"P":283.4,"S":1,` +
		`"c":[1],"t":1587407015152775000}}`
	return []byte(data)
}

func getTestAggregate() []byte {
	const data = `{"data":{"ev":"AM","T":"SPY","v":48526,"av":9663586,"op":282.6,"vw":282.0362,"o":282.13,` +
		`"c":281.94,"h":282.14,"l":281.86,"a":284.4963,"s":1587409020000,"e":1587409080000}}`
	return []byte(data)
}

func TestHandlers(t *testing.T) {
	setup(t)

	// trade
	handlers.MessageHandler(getTestTrade())

	// quote
	handlers.MessageHandler(getTestQuote())

	// aggregate

	handlers.MessageHandler(getTestAggregate())
}
