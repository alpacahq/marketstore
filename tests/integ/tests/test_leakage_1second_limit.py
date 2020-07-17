import os

import numpy as np
import pandas as pd
import pymarketstore as pymkts

client = pymkts.Client(f"http://127.0.0.1:{os.getenv('MARKETSTORE_PORT',5993)}/rpc",
                       grpc=(os.getenv("USE_GRPC", "false") == "true"))


def test_leakage_limit_nrecords():
    data = np.array(
        [(pd.Timestamp("2019-01-01 01:02:03").value / 10 ** 9, 1, 2, 0)],
        dtype=[("Epoch", "i8"), ("Bid", "f4"), ("Ask", "f4"), ("Nanoseconds", "i4")],
    )
    data2 = np.array(
        [(pd.Timestamp("2019-01-01 01:02:03").value / 10 ** 9, 3, 4, 100_000_000)],
        dtype=[("Epoch", "i8"), ("Bid", "f4"), ("Ask", "f4"), ("Nanoseconds", "i4")],
    )
    client.write(data, "DEBUG/1Sec/TICK", isvariablelength=True)
    client.write(data2, "DEBUG/1Sec/TICK", isvariablelength=True)

    param = pymkts.Param(
        "DEBUG",
        "1Sec",
        "TICK",
        start=pd.Timestamp("2019-01-01 01:02:03.100000000"),
        limit=1,
        limit_from_start=True,
    )
    reply = client.query(param)
    df = reply.first().df()
    print(df)
    # if wrong, it means the first tick leaks outside of the interval starting within
    # `start` parameter
    cond1 = (
            (df.shape[0] == 1) and (df.Bid[0] == 3) and (df.Nanoseconds[0] == 100_000_000)
    )

    assert cond1

    param = pymkts.Param(
        "DEBUG",
        "1Sec",
        "TICK",
        end=pd.Timestamp("2019-01-01 01:02:03.000000000"),
        limit=1,
        limit_from_start=False,
    )
    reply = client.query(param)
    df = reply.first().df()
    print(df)
    # if wrong, it means the second tick leaks outside of the interval ending within
    # `end` parameter
    cond2 = (df.shape[0] == 1) and (df.Bid[0] == 1) and (df.Nanoseconds[0] == 0)

    assert cond2
