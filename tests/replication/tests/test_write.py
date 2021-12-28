"""
Integration Test for GRPC client
"""
import pytest
import time
import os

import numpy as np
import pandas as pd

import pymarketstore as pymkts

master_client = pymkts.Client(f"http://127.0.0.1:{os.getenv('MARKETSTORE_PORT',5996)}/rpc",
                              grpc=(os.getenv("USE_GRPC", "false") == "true"))
replica_client = pymkts.Client(f"http://127.0.0.1:{os.getenv('MARKETSTORE_PORT',5999)}/rpc",
                               grpc=(os.getenv("USE_GRPC", "false") == "true"))


def test_write():
    # write -> query -> destroy

    # --- init ---
    destroy("TEST/1Min/Tick")

    # --- write ---
    data = np.array([(pd.Timestamp('2017-01-01 00:00').value / 10 ** 9, 10.0, 20.0)],
                    dtype=[('Epoch', 'i8'), ('High', 'f4'), ('Low', 'f4')])
    master_client.write(data, 'TEST/1Min/OHLCV')

    # --- wait until replication is done ---
    time.sleep(0.1)

    # --- query ---
    resp = replica_client.query(pymkts.Params('TEST', '1Min', 'OHLCV'))
    assert (resp.first().df().values == [10.0, 20.0]).all()

    # --- list_symbols ---
    symbols = replica_client.list_symbols()
    assert "TEST" in symbols

    # --- destroy ---
    destroy("TEST/1Min/OHLCV")


def destroy(tbk: str):
    master_client.destroy(tbk)
    replica_client.destroy(tbk)
    return


def test_tick():
    # --- init ---
    symbol = "TEST"
    timeframe = "1Sec"
    attribute = "Tick"
    destroy("{}/{}/{}".format(symbol, timeframe, attribute))

    # --- write ---
    data = np.array(
        [
            (pd.Timestamp('2017-01-01 00:00:00').value / 10 ** 9, 10.0, 20.0),
            (pd.Timestamp('2017-01-01 00:00:00').value / 10 ** 9, 30.0, 40.0),
        ],
        dtype=[('Epoch', 'i8'), ('Ask', 'f4'), ('Bid', 'f4')]
    )
    master_client.write(data, "{}/{}/{}".format(symbol, timeframe, attribute), isvariablelength=True)

    time.sleep(0.1)

    # --- query ---
    resp = replica_client.query(pymkts.Params(symbol, timeframe, attribute))
    assert (resp.first().df().loc[:, ['Ask', 'Bid']].values == [[10.0, 20.0], [30.0, 40.0]]).all()

    # --- tearDown ---
    destroy("{}/{}/{}".format(symbol, timeframe, attribute))


def test_write_not_allowed_on_replica():
    # --- init ---
    destroy("REPL/1Min/OHLCV")

    # --- try to write to replica ---
    data = np.array([(pd.Timestamp('2017-01-01 00:00').value / 10 ** 9, 10.0, 20.0)],
                    dtype=[('Epoch', 'i8'), ('High', 'f4'), ('Low', 'f4')])
    replica_client.write(data, 'REPL/1Min/OHLCV')

    # --- query and assert "no files returned..." error is returned ---
    with pytest.raises(Exception) as excinfo:
        # resp = {'error': {'code': -32000, 'data': None, 'message': 'no files returned from query parse'}, 'id': '1', 'jsonrpc': '2.0'} # noqa
        # pymarketstore/jsonrpc.py:48: Exception
        replica_client.query(pymkts.Params('REPL', '1Min', 'OHLCV'))
    assert "no files returned from query parse" in str(excinfo.value)

    # --- destroy ---
    destroy("REPL/1Min/OHLCV")
