module github.com/alpacahq/marketstore/v4

go 1.18

require (
	cloud.google.com/go v0.26.0
	code.cloudfoundry.org/bytefmt v0.0.0-20180906201452-2aa6f33b730c
	github.com/adshao/go-binance v0.0.0-20181012004556-e9a4ac01ca48
	github.com/alpacahq/rpc v1.3.0
	github.com/antlr/antlr4 v0.0.0-20181031000400-73836edf1f84
	github.com/bitly/go-simplejson v0.5.0 // indirect
	github.com/bmizerany/assert v0.0.0-20160611221934-b7ed37b82869 // indirect
	github.com/buger/jsonparser v1.0.0
	github.com/chzyer/logex v1.1.10 // indirect
	github.com/chzyer/readline v0.0.0-20180603132655-2972be24d48e
	github.com/chzyer/test v0.0.0-20180213035817-a1ea475d72b1 // indirect
	github.com/eapache/channels v1.1.0
	github.com/eapache/queue v1.1.0 // indirect
	github.com/gobwas/glob v0.2.3
	github.com/golang/mock v1.4.4
	github.com/golang/protobuf v1.5.2
	github.com/google/go-cmp v0.5.5
	github.com/google/go-querystring v1.0.0 // indirect
	github.com/gorilla/websocket v1.4.2
	github.com/json-iterator/go v1.1.12
	github.com/klauspost/compress v1.10.4
	github.com/onsi/ginkgo v1.14.2 // indirect
	github.com/onsi/gomega v1.10.3 // indirect
	github.com/pkg/errors v0.9.1
	github.com/preichenberger/go-gdax v0.0.0-20181027225743-eb74ba719d9a
	github.com/prometheus/client_golang v1.7.1
	github.com/ryszard/goskiplist v0.0.0-20150312221310-2dfbae5fcf46
	github.com/secsy/goftp v0.0.0-20200609142545-aa2de14babf4
	github.com/spf13/cobra v1.0.0
	github.com/stretchr/testify v1.7.0
	github.com/timpalpant/go-iex v0.0.0-20181027174710-0b8a5fdd2ec1
	github.com/vmihailenco/msgpack v4.0.4+incompatible
	go.uber.org/zap v1.15.0
	golang.org/x/tools v0.0.0-20210114065538-d78b04bdf963
	gonum.org/v1/gonum v0.0.0-20190618015908-5dc218f86579
	google.golang.org/grpc v1.29.1
	gopkg.in/check.v1 v1.0.0-20201130134442-10cb98267c6c
	gopkg.in/matryer/try.v1 v1.0.0-20150601225556-312d2599e12e
	gopkg.in/yaml.v2 v2.4.0
)

require (
	github.com/beorn7/perks v1.0.1 // indirect
	github.com/cespare/xxhash/v2 v2.1.2 // indirect
	github.com/cheekybits/is v0.0.0-20150225183255-68e9c0620927 // indirect
	github.com/davecgh/go-spew v1.1.1 // indirect
	github.com/google/gopacket v1.1.16-0.20181023151400-a35e09f9f224 // indirect
	github.com/inconshreveable/mousetrap v1.0.0 // indirect
	github.com/kr/pretty v0.3.0 // indirect
	github.com/kr/text v0.2.0 // indirect
	github.com/matryer/try v0.0.0-20161228173917-9ac251b645a2 // indirect
	github.com/matttproud/golang_protobuf_extensions v1.0.1 // indirect
	github.com/mdlayher/raw v0.0.0-20181016155347-fa5ef3332ca9 // indirect
	github.com/modern-go/concurrent v0.0.0-20180306012644-bacd9c7ef1dd // indirect
	github.com/modern-go/reflect2 v1.0.2 // indirect
	github.com/pmezard/go-difflib v1.0.0 // indirect
	github.com/prometheus/client_model v0.2.0 // indirect
	github.com/prometheus/common v0.10.0 // indirect
	github.com/prometheus/procfs v0.1.3 // indirect
	github.com/rogpeppe/go-internal v1.8.0 // indirect
	github.com/spf13/pflag v1.0.3 // indirect
	go.uber.org/atomic v1.6.0 // indirect
	go.uber.org/multierr v1.5.0 // indirect
	golang.org/x/mod v0.4.1 // indirect
	golang.org/x/net v0.0.0-20210226172049-e18ecbb05110 // indirect
	golang.org/x/sys v0.0.0-20211031064116-611d5d643895 // indirect
	golang.org/x/text v0.3.7 // indirect
	golang.org/x/xerrors v0.0.0-20200804184101-5ec99f83aff1 // indirect
	google.golang.org/appengine v1.4.0 // indirect
	google.golang.org/genproto v0.0.0-20190819201941-24fa4b261c55 // indirect
	google.golang.org/protobuf v1.27.1 // indirect
	gopkg.in/yaml.v3 v3.0.0-20210107192922-496545a6307b // indirect
)

// to avoid "invalid pseudo-version: major version without preceding tag must be v0, not v1" error
replace github.com/go-check/check v1.0.0-20180628173108-788fd7840127 => github.com/go-check/check v0.0.0-20180628173108-788fd7840127
