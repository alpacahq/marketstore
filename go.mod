module github.com/alpacahq/marketstore/v4

go 1.14

require (
	code.cloudfoundry.org/bytefmt v0.0.0-20180906201452-2aa6f33b730c
	github.com/adshao/go-binance v0.0.0-20181012004556-e9a4ac01ca48
	github.com/alpacahq/gopaca v1.16.7
	github.com/alpacahq/rpc v1.3.0
	github.com/antlr/antlr4 v0.0.0-20181031000400-73836edf1f84
	github.com/buger/jsonparser v1.0.0
	github.com/chzyer/readline v0.0.0-20180603132655-2972be24d48e
	github.com/eapache/channels v1.1.0
	github.com/gobwas/glob v0.2.3
	github.com/golang/protobuf v1.4.2
	github.com/gorilla/websocket v1.4.1
	github.com/json-iterator/go v1.1.9
	github.com/klauspost/compress v1.10.4
	github.com/mailru/easyjson v0.7.1
	github.com/onsi/ginkgo v1.14.0 // indirect
	github.com/pkg/errors v0.9.1
	github.com/preichenberger/go-gdax v0.0.0-20181027225743-eb74ba719d9a
	github.com/prometheus/client_golang v1.6.0
	github.com/ryszard/goskiplist v0.0.0-20150312221310-2dfbae5fcf46
	github.com/spf13/cobra v0.0.5
	github.com/stretchr/testify v1.6.1
	github.com/timpalpant/go-iex v0.0.0-20181027174710-0b8a5fdd2ec1
	github.com/valyala/fasthttp v1.14.0
	github.com/vmihailenco/msgpack v4.0.4+incompatible
	go.uber.org/zap v1.15.0
	gonum.org/v1/gonum v0.0.0-20190618015908-5dc218f86579
	google.golang.org/grpc v1.28.0
	gopkg.in/check.v1 v1.0.0-20190902080502-41f04d3bba15
	gopkg.in/matryer/try.v1 v1.0.0-20150601225556-312d2599e12e
	gopkg.in/yaml.v2 v2.3.0
)

// to avoid "invalid pseudo-version: major version without preceding tag must be v0, not v1" error
replace github.com/go-check/check v1.0.0-20180628173108-788fd7840127 => github.com/go-check/check v0.0.0-20180628173108-788fd7840127
