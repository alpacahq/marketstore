module github.com/alpacahq/marketstore/v4

go 1.14

require (
	code.cloudfoundry.org/bytefmt v0.0.0-20180906201452-2aa6f33b730c
	github.com/adshao/go-binance v0.0.0-20181012004556-e9a4ac01ca48
	github.com/alpacahq/rpc v1.3.0
	github.com/antlr/antlr4 v0.0.0-20181031000400-73836edf1f84
	github.com/bitly/go-simplejson v0.5.0 // indirect
	github.com/bmizerany/assert v0.0.0-20160611221934-b7ed37b82869 // indirect
	github.com/buger/jsonparser v0.0.0-20181023193515-52c6e1462ebd
	github.com/cheekybits/is v0.0.0-20150225183255-68e9c0620927 // indirect
	github.com/chzyer/logex v1.1.10 // indirect
	github.com/chzyer/readline v0.0.0-20180603132655-2972be24d48e
	github.com/chzyer/test v0.0.0-20180213035817-a1ea475d72b1 // indirect
	github.com/eapache/channels v1.1.0
	github.com/eapache/queue v1.1.0 // indirect
	github.com/gobwas/glob v0.2.3
	github.com/golang/protobuf v1.4.0
	github.com/google/go-cmp v0.5.2
	github.com/google/go-querystring v1.0.0 // indirect
	github.com/gorilla/websocket v1.4.0
	github.com/inconshreveable/mousetrap v1.0.0 // indirect
	github.com/json-iterator/go v1.1.9
	github.com/klauspost/compress v1.4.1
	github.com/matryer/try v0.0.0-20161228173917-9ac251b645a2 // indirect
	github.com/onsi/ginkgo v1.7.0 // indirect
	github.com/onsi/gomega v1.4.3 // indirect
	github.com/pkg/errors v0.8.1
	github.com/preichenberger/go-gdax v0.0.0-20181027225743-eb74ba719d9a
	github.com/prometheus/client_golang v1.6.0
	github.com/ryszard/goskiplist v0.0.0-20150312221310-2dfbae5fcf46
	github.com/spf13/cobra v0.0.3
	github.com/spf13/pflag v1.0.3 // indirect
	github.com/stretchr/testify v1.4.0
	github.com/timpalpant/go-iex v0.0.0-20181027174710-0b8a5fdd2ec1
	github.com/vmihailenco/msgpack v4.0.1+incompatible
	go.uber.org/atomic v1.4.0 // indirect
	go.uber.org/multierr v1.1.0 // indirect
	go.uber.org/zap v1.10.0
	gonum.org/v1/gonum v0.0.0-20190618015908-5dc218f86579
	google.golang.org/grpc v1.28.0
	gopkg.in/check.v1 v1.0.0-20190902080502-41f04d3bba15
	gopkg.in/matryer/try.v1 v1.0.0-20150601225556-312d2599e12e
	gopkg.in/yaml.v2 v2.2.5
)

// to avoid "invalid pseudo-version: major version without preceding tag must be v0, not v1" error
replace github.com/go-check/check v1.0.0-20180628173108-788fd7840127 => github.com/go-check/check v0.0.0-20180628173108-788fd7840127
