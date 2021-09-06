"""
BUG: Shows that some query return duplicated and unsorted data when query spanning on multiple years with limit arg to other than None.
"""
import os

import numpy as np
import pandas as pd
import pymarketstore as pymkts
import pytest

client = pymkts.Client(f"http://127.0.0.1:{os.getenv('MARKETSTORE_PORT',5993)}/rpc",
                       grpc=(os.getenv("USE_GRPC", "false") == "true"))


@pytest.mark.parametrize(
    "symbol, isvariablelength, timeframe",
    [
        ("TQVD_BUG_1", True, "1Min"),
        ("TQVD_BUG_2", False, "1Min"),
        ("TQVD_BUG_3", True, "1Sec"),
        ("TQVD_BUG_4", False, "1Sec"),
    ],
)
def test_limit_bug_on_multiple_years(symbol, isvariablelength, timeframe):
    data = np.array(
        [
            (pd.Timestamp("2017-01-01 00:00").value / 10 ** 9, 10.0),
            (pd.Timestamp("2018-01-01 00:00").value / 10 ** 9, 11.0),
        ],
        dtype=[("Epoch", "i8"), ("Ask", "f4")],
    )

    client.write(data, f"{symbol}/{timeframe}/TICK", isvariablelength=isvariablelength)

    params = pymkts.Params(symbol, timeframe, "TICK", limit=2)
    res = client.query(params).first().df()

    # tear down
    client.destroy(tbk=f"{symbol}/{timeframe}/TICK")

    res = res.drop(columns="Nanoseconds", axis=1, errors="ignore")

    exp = pd.DataFrame(data).set_index("Epoch")
    exp.index = pd.to_datetime(exp.index, unit="s", utc=True)

    pd.testing.assert_frame_equal(exp, res)

