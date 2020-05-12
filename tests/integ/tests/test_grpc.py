"""
Integration Test for GRPC client
"""
import pytest

import numpy as np
import pandas as pd

import pymarketstore as pymkts

client = pymkts.Client('127.0.0.1:5995', grpc=True)
# client = pymkts.Client('http://127.0.0.1:5993/rpc')


def test_grpc_apis():
    # write -> query -> list_symbols -> destroy

    # --- init ---
    client.destroy("TEST/1Min/Tick")

    # --- write ---
    data = np.array([(pd.Timestamp('2017-01-01 00:00').value / 10 ** 9, 10.0, 20.0)],
                    dtype=[('Epoch', 'i8'), ('High', 'f4'), ('Low', 'f4')])
    client.write(data, 'TEST/1Min/OHLCV')

    # --- query ---
    resp = client.query(pymkts.Params('TEST', '1Min', 'OHLCV'))
    assert (resp.first().df().values == [10.0, 20.0]).all()

    # --- list_symbols ---
    symbols = client.list_symbols()
    assert "TEST" in symbols

    # --- destroy ---
    client.destroy("TEST/1Min/OHLCV")
    assert "TEST" not in client.list_symbols()


def test_grpc_tick():
    # --- init ---
    symbol = "TEST"
    timeframe = "1Sec"
    attribute = "Tick"
    client.destroy("{}/{}/{}".format(symbol, timeframe, attribute))

    # --- write ---
    data = np.array(
        [
            (pd.Timestamp('2017-01-01 00:00:00').value / 10 ** 9, 10.0, 20.0),
            (pd.Timestamp('2017-01-01 00:00:00').value / 10 ** 9, 30.0, 40.0)
        ],
        dtype=[('Epoch', 'i8'), ('Ask', 'f4'), ('Bid', 'f4')]
    )
    client.write(data, "{}/{}/{}".format(symbol, timeframe, attribute), isvariablelength=True)

    # --- query ---
    resp = client.query(pymkts.Params(symbol, timeframe, attribute))
    assert (resp.first().df().loc[:, ['Ask', 'Bid']].values == [[10.0, 20.0], [30.0, 40.0]]).all()

    # --- tearDown ---
    client.destroy("{}/{}/{}".format(symbol, timeframe, attribute))


def test_grpc_range_limit():
    # --- init ---
    symbol = "TEST"
    timeframe = "1Sec"
    attribute = "OHLCV"

    client.destroy("{}/{}/{}".format(symbol, timeframe, attribute))

    # --- write ---
    data = np.array(
        [
            (pd.Timestamp('2017-01-01 00:00:00').value / 10 ** 9, 10.0, 20.0),
            (pd.Timestamp('2017-01-01 01:00:00').value / 10 ** 9, 30.0, 40.0),
            (pd.Timestamp('2017-01-01 02:00:00').value / 10 ** 9, 50.0, 60.0),
            (pd.Timestamp('2017-01-01 03:00:00').value / 10 ** 9, 70.0, 80.0)
        ],
        dtype=[('Epoch', 'i8'), ('High', 'f4'), ('Low', 'f4')]
    )
    client.write(data, "{}/{}/{}".format(symbol, timeframe, attribute))

    # --- start only (start=01:30:00) ---
    resp = client.query(pymkts.Params(symbol, timeframe, attribute, start=pd.Timestamp('2017-01-01 01:30:00')))
    assert (resp.first().df().loc[:, ['High', 'Low']].values == [[50.0, 60.0], [70.0, 80.0]]).all()

    # --- end only (end=01:30:00) ---
    resp = client.query(pymkts.Params(symbol, timeframe, attribute, end=pd.Timestamp('2017-01-01 01:30:00')))
    assert (resp.first().df().loc[:, ['High', 'Low']].values == [[10.0, 20.0], [30.0, 40.0]]).all()

    # --- boundary check (start, end = (01:00:00, 02:00:00), the range includes the boundaries) ---
    resp = client.query(pymkts.Params(symbol, timeframe, attribute, start=pd.Timestamp('2017-01-01 01:00:00'),
                                      end=pd.Timestamp('2017-01-01 02:00:00')))
    assert (resp.first().df().loc[:, ['High', 'Low']].values == [[30.0, 40.0], [50.0, 60.0]]).all()

    # --- limit = 2, limit_from_start=False ---
    resp = client.query(pymkts.Params(symbol, timeframe, attribute, limit=2, limit_from_start=False))
    assert (resp.first().df().loc[:, ['High', 'Low']].values == [[50.0, 60.0], [70.0, 80.0]]).all()

    # --- limit = 2, limit_from_start=True ---
    resp = client.query(pymkts.Params(symbol, timeframe, attribute, limit=2, limit_from_start=True))
    assert (resp.first().df().loc[:, ['High', 'Low']].values == [[10.0, 20.0], [30.0, 40.0]]).all()

    # --- limit = 2, limit_from_start=True ---
    resp = client.query(pymkts.Params(symbol, timeframe, attribute, limit=2, limit_from_start=True))
    assert (resp.first().df().loc[:, ['High', 'Low']].values == [[10.0, 20.0], [30.0, 40.0]]).all()

    # --- all ---
    resp = client.query(pymkts.Params(symbol, timeframe, attribute,
                                      start=pd.Timestamp('2017-01-01 01:00:00'),
                                      end=pd.Timestamp('2017-01-01 03:00:00'),
                                      limit=2,
                                      limit_from_start=True, ))
    assert (resp.first().df().loc[:, ['High', 'Low']].values == [[30.0, 40.0], [50.0, 60.0]]).all()

    # --- tearDown ---
    client.destroy("{}/{}/{}".format(symbol, timeframe, attribute))


def test_grpc_query_all_symbols():
    # --- init ---
    symbol = "TEST"
    symbol2 = "TEST2"
    symbol3 = "TEST3"
    timeframe = "1Sec"
    attribute = "OHLCV"
    tbk = "{}/{}/{}".format(symbol, timeframe, attribute)
    tbk2 = "{}/{}/{}".format(symbol2, timeframe, attribute)
    tbk3 = "{}/{}/{}".format(symbol3, timeframe, attribute)

    client.destroy(tbk)
    client.destroy(tbk2)
    client.destroy(tbk3)

    # --- write ---
    data = np.array([(pd.Timestamp('2017-01-01 00:00:00').value / 10 ** 9, 10.0, 20.0), ],
                    dtype=[('Epoch', 'i8'), ('High', 'f4'), ('Low', 'f4')]
                    )
    data2 = np.array([(pd.Timestamp('2017-01-01 01:00:00').value / 10 ** 9, 30.0, 40.0), ],
                     dtype=[('Epoch', 'i8'), ('High', 'f4'), ('Low', 'f4')]
                     )
    data3 = np.array([(pd.Timestamp('2017-01-01 02:00:00').value / 10 ** 9, 50.0, 60.0), ],
                     dtype=[('Epoch', 'i8'), ('High', 'f4'), ('Low', 'f4')]
                     )
    client.write(data, tbk)
    client.write(data2, tbk2)
    client.write(data3, tbk3)

    # --- query all symbols using * ---
    resp = client.query(pymkts.Params("*", timeframe, attribute, limit=2, ))
    assert len(resp.keys()) >= 3 # TEST, TEST2, TEST3, (and maybe some other test buckets)

    # --- query comma-separated symbol names ---
    print("{},{}/{}/{}".format(symbol, symbol2, timeframe, attribute))
    resp = client.query(
        pymkts.Params([symbol, symbol2], timeframe, attribute, limit=2, ))

    assert set(resp.keys()) == {tbk, tbk2}

    # --- tearDown ---
    client.destroy(tbk)
    client.destroy(tbk2)
    client.destroy(tbk3)
