"""
STATUS: OK
TODO: add more tests for interaction of limits and buckets with ticks.

bug solved by github.com/alpacahq/marketstore/pull/249
MIGRATION_STATUS:OK
"""
import pytest

import numpy as np
import pandas as pd
import pymarketstore as pymkts
from datetime import datetime, timezone
import os


client = pymkts.Client(f"http://127.0.0.1:{os.getenv('MARKETSTORE_PORT',5993)}/rpc",
                       grpc=(os.getenv("USE_GRPC", "false") == "true"))


@pytest.mark.parametrize(
    "symbol, isvariablelength, timeframe",
    [
        ("TEDG_1", True, "1Min"),
        ("TEDG_2", False, "1Min"),
        ("TEDG_3", True, "1Sec"),
        ("TEDG_4", False, "1Sec"),
    ],
)
def test_query_edges(symbol, isvariablelength, timeframe):
    client.destroy(tbk=f"{symbol}/{timeframe}/TICK")

    data = np.array(
        [
            (pd.Timestamp("2017-01-01 00:00").value / 10 ** 9, 10.0),
            (pd.Timestamp("2017-02-01 00:00").value / 10 ** 9, 11.0),
            (pd.Timestamp("2017-03-01 00:00").value / 10 ** 9, 12.0),
        ],
        dtype=[("Epoch", "i8"), ("Ask", "f4")],
    )

    client.write(data, f"{symbol}/{timeframe}/TICK", isvariablelength=isvariablelength)

    params = pymkts.Params(symbol, timeframe, "TICK", limit=1, limit_from_start=True)
    d_start = client.query(params).first().df()

    params = pymkts.Params(symbol, timeframe, "TICK", limit=1, limit_from_start=False)
    d_end = client.query(params).first().df()

    assert len(d_start) == 1
    assert len(d_end) == 1
    assert datetime(2017, 1, 1, 0, 0, 0, tzinfo=timezone.utc) == d_start.index[0]
    assert datetime(2017, 3, 1, 0, 0, 0, tzinfo=timezone.utc) == d_end.index[0]


@pytest.mark.parametrize(
    "symbol, isvariablelength, timeframe",
    [
        ("TEDG_BUG_1", True, "1Min"),
        ("TEDG_BUG_2", False, "1Min"),
        ("TEDG_BUG_3", True, "1Sec"),
        ("TEDG_BUG_4", False, "1Sec"),
    ],
)
def test_query_edges_on_multiple_years(symbol, isvariablelength, timeframe):
    client.destroy(tbk=f"{symbol}/{timeframe}/TICK")

    # original bug fixed by https://github.com/alpacahq/marketstore/pull/249
    data = np.array(
        [
            (pd.Timestamp("2017-01-01 00:00").value / 10 ** 9, 10.0),
            (pd.Timestamp("2018-01-01 00:00").value / 10 ** 9, 11.0),
        ],
        dtype=[("Epoch", "i8"), ("Ask", "f4")],
    )

    client.write(data, f"{symbol}/{timeframe}/TICK", isvariablelength=isvariablelength)

    params = pymkts.Params(symbol, timeframe, "TICK", limit=1, limit_from_start=True)
    d_start = client.query(params).first().df()

    params = pymkts.Params(symbol, timeframe, "TICK", limit=1, limit_from_start=False)
    d_end = client.query(params).first().df()

    assert len(d_start) == 1
    assert len(d_end) == 1
    assert datetime(2017, 1, 1, 0, 0, 0, tzinfo=timezone.utc) == d_start.index[0]
    assert datetime(2018, 1, 1, 0, 0, 0, tzinfo=timezone.utc) == d_end.index[0]
