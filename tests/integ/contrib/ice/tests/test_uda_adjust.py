import pymarketstore as pymkts
import os
import pandas as pd 

client = pymkts.Client(f"http://127.0.0.1:{os.getenv('MARKETSTORE_PORT',5993)}/rpc",
                       grpc=(os.getenv("USE_GRPC", "false") == "true"))

path = os.path.dirname(__file__)

def test_raw_price_data():
    params = pymkts.Params('AAPL', '1D', 'OHLCV')
    mkts_raw = client.query(params).first().df()
    
    aapl_raw = pd.read_csv(os.path.join(path, "aapl_raw.csv"), parse_dates=['Epoch'], index_col=['Epoch'])
    aapl_raw.index = aapl_raw.index.tz_localize('UTC').tz_convert('US/Eastern').tz_localize(None)
    
    mkts_raw.index = mkts_raw.index.tz_localize(None)

    assert mkts_raw.round(3).equals(aapl_raw.round(3))


def test_adjusted_price_data():
    params = pymkts.Params('AAPL', '1D', 'OHLCV')
    params.functions = ["adjust('split')"]
    mkts = client.query(params).first().df()
    
    csv = pd.read_csv(os.path.join(path, "aapl_adj.csv"), parse_dates=['Epoch'], index_col=['Epoch'])
    csv.index = csv.index.tz_localize('UTC').tz_convert('US/Eastern').tz_localize(None)
    mkts.index = mkts.index.tz_localize(None)
    
    mkts = mkts.round(3)
    csv = csv.round(3)
    print(csv.loc['2020-08-25':].head(5))
    print(mkts.loc['2020-08-25':].head(5))
    sdf
    assert mkts['Close'].equals(csv['Close'])
    assert mkts['Open'].equals(csv['Open'])
    assert mkts['High'].equals(csv['High'])
    assert mkts['Low'].equals(csv['Low'])
    



