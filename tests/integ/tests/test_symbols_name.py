"""
status:NOK, the name of the columns is being transformed and has an unexpected behaviour.
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
    "symbol, column_name",
    [
        # ('TEST_SYMBOLS_NAME_1', 'Ask'),
        # ('TEST_SYMBOLS_NAME_2', '_ask'),
        # ('TEST_SYMBOLS_NAME_3', '_Ask'),
        # ('TEST_SYMBOLS_NAME_4', 'L1Ask'),
        ('TEST_SYMBOLS_NAME_5', 'l1Ask'),
        # ('TEST_SYMBOLS_NAME_6', 'L1ask'),
        # ('TEST_SYMBOLS_NAME_7', 'l1ask'),
        # ('TEST_SYMBOLS_NAME_8', 'L1-Ask'),
        # ('TEST_SYMBOLS_NAME_9', 'l1-Ask'),
        # ('TEST_SYMBOLS_NAME_10', 'L1-ask'),
        # ('TEST_SYMBOLS_NAME_11', 'l1-ask'),
        # ('TEST_SYMBOLS_NAME_12', 'L1_ask'),
        # ('TEST_SYMBOLS_NAME_13', 'l1_ask'),
    ]
)
def test_symbols_name(symbol, column_name):
    client.destroy(tbk="{}/1Min/Tick".format(symbol))

    data = np.array(
        [(pd.Timestamp("2017-01-01 00:00").value / 10 ** 9, 10.0)],
        dtype=[("Epoch", "i8"), (column_name, "f4")],
    )

    client.write(data, "{}/1Min/Tick".format(symbol))

    d2 = client.query(pymkts.Params(symbol, "1Min", "Tick")).first().df()
    print("Length of result: ", d2.shape[0])
    assert d2.shape[0] == 1
    assert (
            datetime(2017, 1, 1, 0, 0, 0, tzinfo=timezone.utc).timestamp()
            == d2.index[0].timestamp()
    )
    assert d2.columns.tolist() == [column_name]
