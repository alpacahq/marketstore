
"""
Integration Test for 1Sec timeframe
"""
import pytest

import numpy as np
import pandas as pd

import pymarketstore as pymkts

# Constants
DATA_TYPE_TICK = [('Epoch', 'i8'), ('Bid', 'f4'), ('Ask', 'f4'), ('Nanoseconds', 'i4')]
DATA_TYPE_CANDLE = [('Epoch', 'i8'), ('Open', 'f8'), ('High', 'f8'), ('Low', 'f8'), ('Close', 'f8'), ('Volume', 'f8')]
MARKETSTORE_HOST = "localhost"
MARKETSTORE_PORT = 5993

client = pymkts.Client('http://localhost:5993/rpc')


def timestamp(datestr):
    return int(pd.Timestamp(datestr).value / 10 ** 9)


@pytest.mark.parametrize('symbol, data', [
    ('TEST_SIMPLE_TICK',                [(timestamp('2019-01-01 00:00:00'), 1, 2, 3),  # epoch, ask, bid, nanosecond
                                         (timestamp('2019-01-01 00:00:01'), 4, 5, 6),
                                         (timestamp('2019-12-31 23:59:59'), 7, 8, 9)]),
    ('TEST_MULTIPLE_TICK_IN_TIMEFRAME', [(timestamp('2019-01-01 18:00:00'), 1, 1, 1000),
                                         (timestamp('2019-01-01 18:00:00'), 2, 2, 2000)]),
    ('TEST_DUPLICATE_INDEX',            [(timestamp('2019-01-01 12:00:00'), 1, 1, 3),
                                         (timestamp('2019-01-01 12:00:00'), 2, 2, 3)])
])
def test_1sec_tf_tick(symbol, data):
    # ---- given ----
    client.write(np.array(data, dtype=DATA_TYPE_TICK), "{}/1Sec/TICK".format(symbol), isvariablelength=True)

    # ---- when ----
    reply = client.query(pymkts.Params(symbol, '1Sec', 'TICK', limit=10))

    # ---- then ----
    data_without_epochs = [record[1:] for record in data]
    assert (reply.first().df().values == data_without_epochs).all()


@pytest.mark.parametrize('symbol, data', [
    ('TEST_SIMPLE_OHLCV',                [(timestamp('2019-01-01 00:00:00'), 1.0, 2.0, 3.0, 4.0, 5.0 ),  # epoch, Open, High, Low, Close, Volume
                                         (timestamp('2019-01-01 00:00:01'), 1.0, 2.0, 3.0, 4.0, 5.0 ),
                                         (timestamp('2019-12-31 23:59:59'), 6.0, 7.0, 8.0, 9.0, 10.0)]),
])
def test_1sec_tf_candle(symbol, data):
    # ---- given ----
    print(client.write(np.array(data, dtype=DATA_TYPE_CANDLE), "{}/1Sec/OHLCV".format(symbol), isvariablelength=False))

    # ---- when ----
    reply = client.query(pymkts.Params(symbol, '1Sec', 'OHLCV', limit=10))

    # ---- then ----
    data_without_epochs = [record[1:] for record in data]
    assert (reply.first().df().values == data_without_epochs).all()
