package polygonconfig

type FetcherConfig struct {
	// AddTickCountToBars controls if TickCnt is added to the schema for Bars or not
	AddTickCountToBars bool `json:"add_bar_tick_count,omitempty"`
	// polygon API key for authenticating with their APIs
	APIKey string `json:"api_key"`
	// polygon API base URL in case it is being proxied
	// (defaults to https://api.polygon.io/)
	BaseURL string `json:"base_url"`
	// websocket servers for Polygon, default is: "wss://socket.polygon.io"
	WSServers string `json:"ws_servers"`
	// list of data types to subscribe to (one of bars, quotes, trades)
	DataTypes []string `json:"data_types"`
	// list of symbols that are important
	Symbols []string `json:"symbols"`
	// time string when to start first time, in "YYYY-MM-DD HH:MM" format
	// if it is restarting, the start is the last written data timestamp
	// otherwise, it starts from the latest streamed bar
	QueryStart string `json:"query_start"`
}
