package frontend

// QueryRequestBuilder is a builder for QueryRequest to set
// various parameters flexibly.
//
//   qr := NewQueryRequestBuilder("TSLA/1D/OHLCV").LimitRecourdCount(100).End()
//
type QueryRequestBuilder struct {
	qr *QueryRequest
}

func NewQueryRequestBuilder(destination string) *QueryRequestBuilder {
	return &QueryRequestBuilder{
		qr: &QueryRequest{
			Destination: destination,
		},
	}
}

//
func (b *QueryRequestBuilder) EpochStart(value int64) *QueryRequestBuilder {
	b.qr.EpochStart = &value
	return b
}

func (b *QueryRequestBuilder) EpochEnd(value int64) *QueryRequestBuilder {
	b.qr.EpochEnd = &value
	return b
}

func (b *QueryRequestBuilder) LimitRecordCount(value int) *QueryRequestBuilder {
	b.qr.LimitRecordCount = &value
	return b
}

func (b *QueryRequestBuilder) LimitFromStart(value bool) *QueryRequestBuilder {
	b.qr.LimitFromStart = &value
	return b
}

func (b *QueryRequestBuilder) Functions(value []string) *QueryRequestBuilder {
	b.qr.Functions = value
	return b
}

func (b *QueryRequestBuilder) End() QueryRequest {
	return *b.qr
}
