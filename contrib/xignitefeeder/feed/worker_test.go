package feed

// MockedAPIClient returns example quote data
//type MockedAPIClient struct{}

//func (m *MockedAPIClient) GetRealTimeQuotes(identifiers []string) (api.GetQuotesResponse, error) {
//	return api.GetQuotesResponse{
//		[]api.EquityQuote{
//			{
//				Outcome: "Success",
//				Security: api.Security{
//					Symbol: "FOOBAR",
//				},
//				Quote: api.Quote{
//					DateTime: "2019/04/05 15:00:00",
//					Ask:      123.4,
//					Bid:      567.8,
//				},
//			},
//			{
//				Outcome: "Success",
//				Security: api.Security{
//					Symbol: "HOGE",
//				},
//				Quote: api.Quote{
//					DateTime: "2019/03/04 12:34:56",
//					Ask:      123,
//					Bid:      456,
//				},
//			},
//		},
//	}, nil
//}
//
//// MockedErrorAPIClient returns an error
//type MockedErrorAPIClient struct{}
//
//func (m *MockedErrorAPIClient) GetRealTimeQuotes(identifiers []string) (api.GetQuotesResponse, error) {
//	return api.GetQuotesResponse{}, errors.New("fail")
//}
//
//// MockedErrorCSMWriter returns an error
//type MockedCSMWriter struct{}
//
//func (m *MockedCSMWriter) Write(csm io.ColumnSeriesMap) error {
//	return nil
//}
//
//type MockedErrorCSMWriter struct{}
//
//func (m *MockedErrorCSMWriter) Write(csm io.ColumnSeriesMap) error {
//	return errors.New("fail")
//}
//
//func TestWorker_try(t *testing.T) {
//	type fields struct {
//		APIClient   api.Client
//		CSMWriter   CSMWriter
//		Timeframe   string
//		Intervals int
//	}
//	tests := []struct {
//		name    string
//		fields  fields
//		wantErr bool
//	}{
//		// Test Cases
//		{
//			name: "Normal",
//			fields: fields{
//				APIClient:   &MockedAPIClient{},
//				CSMWriter:   &MockedCSMWriter{},
//				Timeframe:   "1Sec",
//				Identifiers: []string{"1234.FOOBAR"},
//			},
//			wantErr: false,
//		},
//		{
//			name: "When Xignite API returns an error, Worker fails",
//			fields: fields{
//				APIClient:   &MockedErrorAPIClient{},
//				CSMWriter:   &MockedCSMWriter{},
//				Timeframe:   "1Sec",
//				Identifiers: []string{"1234.FOOBAR"},
//			},
//			wantErr: true,
//		},
//		{
//			name: "When Marketstore returns an error, Worker fails",
//			fields: fields{
//				APIClient:   &MockedAPIClient{},
//				CSMWriter:   &MockedErrorCSMWriter{},
//				Timeframe:   "1Sec",
//				Identifiers: []string{"1234.FOOBAR"},
//			},
//			wantErr: true,
//		},
//	}
//	for _, tt := range tests {
//		t.Run(tt.name, func(t *testing.T) {
//			w := &Worker{
//				APIClient:   tt.fields.APIClient,
//				CSMWriter:   tt.fields.CSMWriter,
//				Timeframe:   tt.fields.Timeframe,
//				Interval: tt.fields.Identifiers,
//			}
//			if err := w.try(); (err != nil) != tt.wantErr {
//				t.Errorf("Worker.try() error = %v, wantErr %v", err, tt.wantErr)
//			}
//		})
//	}
//}
//
//func TestConvertDateTimeToEpoch(t *testing.T) {
//	tests := []struct {
//		datetime  string
//		wantEpoch int64
//		wantErr   bool
//	}{
//		// Test cases.
//		{"1970/01/01 00:00:00",
//			0,
//			false,
//		},
//		{"1970/01/01 01:00:00",
//			3600,
//			false,
//		},
//		// unexpected layout
//		{"1970-01-01 01:00:00",
//			0,
//			true,
//		},
//	}
//	for _, tt := range tests {
//		gotEpoch, err := ConvertDateTimeToEpoch(tt.datetime)
//		if (err != nil) != tt.wantErr {
//			t.Errorf("ConvertDateTimeToEpoch() error = %v, wantErr %v", err, tt.wantErr)
//			return
//		}
//		if gotEpoch != tt.wantEpoch {
//			t.Errorf("ConvertDateTimeToEpoch() = %v, want %v", gotEpoch, tt.wantEpoch)
//		}
//	}
//}
