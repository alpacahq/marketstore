"""
Integration Test for 1Day timeframe
"""
import os

import numpy as np
import pandas as pd
import pymarketstore as pymkts
import pytest

# Constants
DATA_TYPE_TICK = [('Epoch', 'i8'), ('Bid', 'f4'), ('Ask', 'f4'), ('Nanoseconds', 'i4')]
DATA_TYPE_CANDLE = [('Epoch', 'i8'), ('Open', 'f8'), ('High', 'f8'), ('Low', 'f8'), ('Close', 'f8'), ('Volume', 'f8')]
MARKETSTORE_HOST = "localhost"
MARKETSTORE_PORT = 5993

client = pymkts.Client(f"http://127.0.0.1:{os.getenv('MARKETSTORE_PORT', 5993)}/rpc",
                       grpc=(os.getenv("USE_GRPC", "false") == "true"))


def timestamp(datestr):
    return int(pd.Timestamp(datestr).value / 10 ** 9)


@pytest.mark.parametrize('symbol, data', [
    ('TEST_SIMPLE_TICK', [(timestamp('2019-01-01 00:00:00'), 1, 2, 0),  # epoch, ask, bid, nanosecond
                          (timestamp('2019-01-01 12:00:00'), 3, 4, 0),
                          (timestamp('2019-12-31 18:00:00'), 5, 6, 0)]),
    ('TEST_MULTIPLE_TICK_IN_TIMEFRAME', [(timestamp('2019-01-01 06:00:00'), 1, 1, 0),
                                         (timestamp('2019-01-01 06:00:00'), 2, 2, 0)]),
    ('TEST_DUPLICATE_INDEX', [(timestamp('2019-01-01 00:00:00'), 1, 1, 0),
                              (timestamp('2019-01-01 00:00:00'), 2, 2, 0)])
])
def test_1day_tf_tick(symbol, data):
    # ---- given ----
    tbk = "{}/1D/TICK".format(symbol)
    client.destroy(tbk)  # setup

    client.write(np.array(data, dtype=DATA_TYPE_TICK), tbk, isvariablelength=True)

    # ---- when ----
    reply = client.query(pymkts.Params(symbol, '1D', 'TICK', limit=10))

    # ---- then ----
    data_without_epochs = [record[1:] for record in data]
    assert (reply.first().df().values == data_without_epochs).all()


@pytest.mark.parametrize('symbol, data', [
    ('TEST_SIMPLE_OHLCV',
     [(timestamp('2019-01-01 00:00:00'), 1.0, 2.0, 3.0, 4.0, 5.0),  # epoch, Open, High, Low, Close, Volume
      (timestamp('2019-01-02 00:00:00'), 1.0, 2.0, 3.0, 4.0, 5.0),
      (timestamp('2019-12-31 00:00:00'), 6.0, 7.0, 8.0, 9.0, 10.0)]),
])
def test_1sec_tf_candle(symbol, data):
    # ---- given ----
    tbk = "{}/1D/OHLCV".format(symbol)
    client.destroy(tbk)  # setup

    print(client.write(np.array(data, dtype=DATA_TYPE_CANDLE), tbk, isvariablelength=False))

    # ---- when ----
    reply = client.query(pymkts.Params(symbol, '1D', 'OHLCV', limit=10))

    # ---- then ----
    data_without_epochs = [record[1:] for record in data]
    assert (reply.first().df().values == data_without_epochs).all()
