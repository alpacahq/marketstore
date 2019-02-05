module github.com/alpacahq/marketstore

require (
	code.cloudfoundry.org/bytefmt v0.0.0-20180906201452-2aa6f33b730c
	github.com/Shopify/goreferrer v0.0.0-20181106222321-ec9c9a553398 // indirect
	github.com/adshao/go-binance v0.0.0-20181012004556-e9a4ac01ca48
	github.com/alpacahq/rpc v1.3.0
	github.com/alpacahq/slait v1.1.7
	github.com/antlr/antlr4 v0.0.0-20181031000400-73836edf1f84
	github.com/bitly/go-simplejson v0.5.0 // indirect
	github.com/bmizerany/assert v0.0.0-20160611221934-b7ed37b82869 // indirect
	github.com/buger/jsonparser v0.0.0-20181023193515-52c6e1462ebd
	github.com/cheekybits/is v0.0.0-20150225183255-68e9c0620927 // indirect
	github.com/chzyer/logex v1.1.10 // indirect
	github.com/chzyer/readline v0.0.0-20180603132655-2972be24d48e
	github.com/chzyer/test v0.0.0-20180213035817-a1ea475d72b1 // indirect
	github.com/eapache/channels v1.1.0
	github.com/flosch/pongo2 v0.0.0-20181225140029-79872a7b2769 // indirect
	github.com/gobwas/glob v0.2.3
	github.com/gocarina/gocsv v0.0.0-20180809181117-b8c38cb1ba36
	github.com/gorilla/websocket v1.4.0
	github.com/inconshreveable/mousetrap v1.0.0 // indirect
	github.com/iris-contrib/blackfriday v2.0.0+incompatible // indirect
	github.com/iris-contrib/formBinder v0.0.0-20190104093907-fbd5963f41e1 // indirect
	github.com/kataras/iris v11.1.0+incompatible // indirect
	github.com/kataras/pio v0.0.0-20190103105442-ea782b38602d // indirect
	github.com/klauspost/compress v1.4.1
	github.com/klauspost/cpuid v1.2.0 // indirect
	github.com/matryer/try v0.0.0-20161228173917-9ac251b645a2 // indirect
	github.com/microcosm-cc/bluemonday v1.0.2 // indirect
	github.com/moul/http2curl v1.0.0 // indirect
	github.com/nats-io/gnatsd v1.3.0 // indirect
	github.com/nats-io/go-nats v1.6.0
	github.com/nats-io/nuid v1.0.0 // indirect
	github.com/onsi/ginkgo v1.7.0 // indirect
	github.com/onsi/gomega v1.4.3 // indirect
	github.com/preichenberger/go-gdax v0.0.0-20181027225743-eb74ba719d9a
	github.com/shurcooL/sanitized_anchor_name v1.0.0 // indirect
	github.com/smartystreets/goconvey v0.0.0-20181108003508-044398e4856c // indirect
	github.com/spf13/cobra v0.0.3
	github.com/spf13/pflag v1.0.3 // indirect
	github.com/stretchr/objx v0.1.1 // indirect
	github.com/timpalpant/go-iex v0.0.0-20181027174710-0b8a5fdd2ec1
	github.com/valyala/fasthttp v1.0.0
	github.com/vmihailenco/msgpack v4.0.1+incompatible
	go.uber.org/atomic v1.3.2 // indirect
	go.uber.org/multierr v1.1.0 // indirect
	go.uber.org/zap v1.9.1
	golang.org/x/sys v0.0.0-20181116161606-93218def8b18 // indirect
	google.golang.org/appengine v1.3.0 // indirect
	gopkg.in/check.v1 v1.0.0-20180628173108-788fd7840127
	gopkg.in/matryer/try.v1 v1.0.0-20150601225556-312d2599e12e
	gopkg.in/yaml.v2 v2.2.2
)

replace (
	github.com/alpacahq/slait v1.1.7 => github.com/xmurobi/slait v1.1.8
	go.uber.org/atomic v1.3.2 => github.com/uber-go/atomic v1.3.2
	go.uber.org/multierr v1.1.0 => github.com/uber-go/multierr v1.1.0
	go.uber.org/zap v1.9.1 => github.com/uber-go/zap v1.9.1
	golang.org/x/crypto v0.0.0-20180927165925-5295e8364332 => github.com/golang/crypto v0.0.0-20180927165925-5295e8364332
	golang.org/x/crypto v0.0.0-20181106152344-bfa7d42eb568 => github.com/golang/crypto v0.0.0-20181106152344-bfa7d42eb568
	golang.org/x/net v0.0.0-20180724234803-3673e40ba225 => github.com/golang/net v0.0.0-20180724234803-3673e40ba225
	golang.org/x/net v0.0.0-20180906233101-161cd47e91fd => github.com/golang/net v0.0.0-20180906233101-161cd47e91fd
	golang.org/x/net v0.0.0-20181023162649-9b4f9f5ad519 => github.com/golang/net v0.0.0-20181023162649-9b4f9f5ad519
	golang.org/x/net v0.0.0-20181102091132-c10e9556a7bc => github.com/golang/net v0.0.0-20181102091132-c10e9556a7bc
	golang.org/x/sync v0.0.0-20180314180146-1d60e4601c6f => github.com/golang/sync v0.0.0-20180314180146-1d60e4601c6f
	golang.org/x/sys v0.0.0-20180909124046-d0be0721c37e => github.com/golang/sys v0.0.0-20180909124046-d0be0721c37e
	golang.org/x/sys v0.0.0-20180928133829-e4b3c5e90611 => github.com/golang/sys v0.0.0-20180928133829-e4b3c5e90611
	golang.org/x/sys v0.0.0-20181024145615-5cd93ef61a7c => github.com/golang/sys v0.0.0-20181024145615-5cd93ef61a7c
	golang.org/x/text v0.3.0 => github.com/golang/text v0.3.0
	google.golang.org/appengine v1.3.0 => github.com/golang/appengine v1.3.0
)
