package symbols_test

// AAPL, ACN, ADBE. restriction info should be ignored.
const mockTradableStocksJSON = `
{
  "update_datetime": "2022-03-28T08:30:29.222960Z",
  "data": {
    "AAPL": {
      "restriction": [
        {
          "from": "2022-03-17T11:48:31.246Z",
          "to": "9999-12-31T14:59:59Z",
          "stock_order_side": "",
          "restriction_reason": "test",
          "unrestriction_reason": "na"
        }
      ]
    },
    "ACN": {
      "restriction": []
    },
    "ADBE": {
      "restriction": []
    }
  }
}
`

const unexpectedJSON = `
{"abc": ["d", "e","f"]}aaaa
`
