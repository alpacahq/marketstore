import pymarketstore as pymkts
import os
import pandas as pd 

client = pymkts.Client(f"http://127.0.0.1:{os.getenv('MARKETSTORE_PORT',5993)}/rpc",
                       grpc=(os.getenv("USE_GRPC", "false") == "true"))

path = os.path.dirname(__file__)

def test_raw_price_data():
    params = pymkts.Params('AAPL', '1D', 'OHLCV')
    mkts = client.query(params).first().df()
    
    csv = pd.read_csv(os.path.join(path, "aapl_raw.csv"), parse_dates=['Epoch'], index_col=['Epoch'])
    csv.index = csv.index.tz_localize('UTC').tz_convert('US/Eastern').tz_localize(None)
    mkts.index = mkts.index.tz_localize(None)

    mkts = mkts.round(3)
    csv = csv.round(3)
    
    # comparing columns separately for easier debug 
    assert mkts['Close'].equals(csv['Close'])
    assert mkts['Open'].equals(csv['Open'])
    assert mkts['High'].equals(csv['High'])
    assert mkts['Low'].equals(csv['Low'])
    assert mkts['Volume'].equals(csv['Volume'])


def test_adjusted_price_data():
    params = pymkts.Params('AAPL', '1D', 'OHLCV')
    params.functions = ["adjust('split')"]
    mkts = client.query(params).first().df()
    
    csv = pd.read_csv(os.path.join(path, "aapl_adj.csv"), parse_dates=['Epoch'], index_col=['Epoch'])
    csv.index = csv.index.tz_localize('UTC').tz_convert('US/Eastern').tz_localize(None)
    mkts.index = mkts.index.tz_localize(None)
    
    mkts = mkts.round(3)
    csv = csv.round(3)
   
    # comparing columns separately for easier debug 
    assert mkts['Close'].equals(csv['Close'])
    assert mkts['Open'].equals(csv['Open'])
    assert mkts['High'].equals(csv['High'])
    assert mkts['Low'].equals(csv['Low'])
    assert mkts['Volume'].equals(csv['Volume'])
    

def test_corporate_actions():
    params = pymkts.Params('AAPL', '1D', 'ACTIONS')
    ca = client.query(params).first().df()

    assert len(ca) == 1
    assert ca.TextNumber[0] == 2103357
    assert ca.Rate[0] == 4.0




