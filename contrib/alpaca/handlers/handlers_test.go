package handlers_test

import (
	"testing"

	"github.com/alpacahq/marketstore/v4/executor"
	"github.com/alpacahq/marketstore/v4/internal/di"
	"github.com/alpacahq/marketstore/v4/utils"

	"github.com/alpacahq/marketstore/v4/contrib/alpaca/handlers"
)

func setup(t *testing.T) {
	t.Helper()

	cfg := utils.NewDefaultConfig(t.TempDir())
	cfg.WALBypass = true
	cfg.BackgroundSync = false
	c := di.NewContainer(cfg)
	executor.NewInstanceSetup(c.GetCatalogDir(), c.GetInitWALFile())
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
