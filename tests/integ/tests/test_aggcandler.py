import pytest
import os

import numpy as np
import pandas as pd

import pymarketstore as pymkts

client = pymkts.Client(f"http://127.0.0.1:{os.getenv('MARKETSTORE_PORT', 5993)}/rpc",
                       grpc=(os.getenv("USE_GRPC", "false") == "true"))


def timestamp(datestr):
    return int(pd.Timestamp(datestr).value / 10 ** 9)


symbol = "TEST_CC"
data_type = [('Epoch', 'i8'), ('Open', 'f4'), ('High', 'f4'), ('Low', 'f4'), ('Close', 'f4')]
data = [(timestamp('2020-01-01 00:00:00'), 20.0, 40.0, 10.0, 30.0),
        (timestamp('2020-01-01 01:00:00'), 40.0, 80.0, 20.0, 60.0),
        (timestamp('2020-01-01 02:00:00'), 60.0, 120.0, 30.0, 90.0),
        (timestamp('2020-01-01 03:00:00'), 80.0, 160.0, 40.0, 120.0),
        ]


@pytest.mark.parametrize('aggfunc, limit, limit_from_start, exp_open, exp_high, exp_low, exp_close', [
    (["candlecandler('2H',Open,High,Low,Close)"], 100, True, 20, 80, 10, 60),  # 00:00 - 02:00
    (["candlecandler('4H',Open,High,Low,Close)"], 100, True, 20, 160, 10, 120),  # 00:00 - 04:00
])
def test_candlecandler(aggfunc, limit, limit_from_start, exp_open, exp_high, exp_low, exp_close):
    # ---- given ----
    tbk = "{}/1Sec/OHLC".format(symbol)
    client.destroy(tbk)  # setup

    client.write(np.array(data, dtype=data_type), tbk, isvariablelength=False)

    # ---- when ----
    agg_reply = client.query(pymkts.Params(symbol, '1Sec', 'OHLC', limit=limit, limit_from_start=limit_from_start,
                                           functions=aggfunc))

    # ---- then ----
    ret = agg_reply.first().df()
    assert ret["Open"][0] == exp_open
    assert ret["High"][0] == exp_high
    assert ret["Low"][0] == exp_low
    assert ret["Close"][0] == exp_close


symbol2 = "TEST_TC"
data_type2 = [('Epoch', 'i8'), ('Ask', 'f4'), ('Bid', 'f4')]
data2 = [(timestamp('2020-01-01 00:00:00'), 10.0, 20.0),
         (timestamp('2020-01-01 01:00:00'), 30.0, 40.0),
         (timestamp('2020-01-01 02:00:00'), 50.0, 60.0),
         (timestamp('2020-01-01 03:00:00'), 70.0, 80.0),
         ]


@pytest.mark.parametrize('aggfunc, limit, limit_from_start, exp_open, exp_high, exp_low, exp_close', [
    (["tickcandler('2H', Ask)"], 100, True, 10, 30, 10, 30),  # 00:00 - 02:00
    (["tickcandler('2H', Bid)"], 100, True, 20, 40, 20, 40),  # 00:00 - 02:00
    (["tickcandler('4H', Ask)"], 100, True, 10, 70, 10, 70),  # 00:00 - 04:00
])
def test_tickcandler(aggfunc, limit, limit_from_start, exp_open, exp_high, exp_low, exp_close):
    # ---- given ----
    tbk = "{}/1Sec/TICK".format(symbol2)
    client.destroy(tbk)  # setup

    client.write(np.array(data2, dtype=data_type2), tbk, isvariablelength=False)

    # ---- when ----
    agg_reply = client.query(pymkts.Params(symbol2, '1Sec', 'TICK', limit=limit, limit_from_start=limit_from_start,
                                           functions=aggfunc))

    # ---- then ----
    ret = agg_reply.first().df()
    assert ret["Open"][0] == exp_open
    assert ret["High"][0] == exp_high
    assert ret["Low"][0] == exp_low
    assert ret["Close"][0] == exp_close
