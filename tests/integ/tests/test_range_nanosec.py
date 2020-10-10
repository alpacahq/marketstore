"""
Integration Test for nanosecond support for start/end query parameters
"""
import pytest
import os

import numpy as np
import pandas as pd

import pymarketstore as pymkts

# Constants
DATA_TYPE_NANOSEC = [('Epoch', 'i8'), ('Bid', 'f4'), ('Ask', 'f4'), ('Nanoseconds', 'i4')]
MARKETSTORE_HOST = "localhost"

client = pymkts.Client(f"http://127.0.0.1:{os.getenv('MARKETSTORE_PORT',5993)}/rpc",
                       grpc=(os.getenv("USE_GRPC", "false") == "true"))


def timestamp(datestr):
    return int(pd.Timestamp(datestr).value / 10 ** 9)


@pytest.mark.parametrize('symbol, data, start, end, limit, limit_from_start, response', [
    # Epoch, Bid, Ask, Nanoseconds
    ('TEST_NANOSEC_RANGE1', [(timestamp('2019-01-01 01:02:03'), 1.0, 2.0, 100000000),
                             (timestamp('2019-01-01 01:02:03'), 1.0, 2.0, 200000000),
                             (timestamp('2019-01-01 01:02:03'), 1.0, 2.0, 300000000),
                             (timestamp('2019-01-01 01:02:03'), 1.0, 2.0, 400000000),
                             (timestamp('2019-01-01 01:02:03'), 1.0, 2.0, 500000000),
                             ],
     '2019-01-01 01:02:03.200000000',
     '2019-01-01 01:02:03.400000000',
     10,
     True,
     [[1.0, 2.0, 200000000],
      [1.0, 2.0, 300000000],
      [1.0, 2.0, 400000000], ]),
    # -------------------------
    # with limit value
    ('TEST_NANOSEC_RANGE2', [(timestamp('2019-01-01 01:02:03'), 1.0, 2.0, 100000000),
                             (timestamp('2019-01-01 01:02:03'), 1.0, 2.0, 200000000),
                             (timestamp('2019-01-01 01:02:03'), 1.0, 2.0, 300000000),
                             (timestamp('2019-01-01 01:02:03'), 1.0, 2.0, 400000000),
                             (timestamp('2019-01-01 01:02:03'), 1.0, 2.0, 500000000),
                             ],
     '2019-01-01 01:02:03.200000000',
     '2019-01-01 01:02:03.400000000',
     2,
     True,
     [[1.0, 2.0, 200000000],
      [1.0, 2.0, 300000000], ]),
    # -------------------------
    # with limit_from_start = False
    ('TEST_NANOSEC_RANGE3', [(timestamp('2019-01-01 01:02:03'), 1.0, 2.0, 100000000),
                             (timestamp('2019-01-01 01:02:03'), 1.0, 2.0, 200000000),
                             (timestamp('2019-01-01 01:02:03'), 1.0, 2.0, 300000000),
                             (timestamp('2019-01-01 01:02:03'), 1.0, 2.0, 400000000),
                             (timestamp('2019-01-01 01:02:03'), 1.0, 2.0, 500000000),
                             ],
     '2019-01-01 01:02:03.200000000',
     '2019-01-01 01:02:03.400000000',
     2,
     False,
     [[1.0, 2.0, 300000000],
      [1.0, 2.0, 400000000], ])
])
def test_nanosec_range(symbol, data, start, end, limit, limit_from_start, response):
    # ---- given ----
    tbk = "{}/1Sec/TICK".format(symbol)
    client.destroy(tbk)  # setup

    print(client.write(np.array(data, dtype=DATA_TYPE_NANOSEC), tbk, isvariablelength=True))

    # ---- when ----
    reply = client.query(pymkts.Params(symbol, '1Sec', 'TICK',
                                       start=start,
                                       end=end,
                                       limit=limit,
                                       limit_from_start=limit_from_start,
                                       ))

    # ---- then ----
    ret_df = reply.first().df()
    assert (response == ret_df.values).all()

    client.destroy(tbk)
