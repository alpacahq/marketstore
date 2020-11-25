"""
Integration Test for Column Coercion feature
"""
import pytest
import os

import numpy as np
import pandas as pd

import pymarketstore as pymkts

use_grpc = os.getenv("USE_GRPC", "false") == "true"
client = pymkts.Client(f"http://127.0.0.1:{os.getenv('MARKETSTORE_PORT',5993)}/rpc",
                       grpc=(use_grpc))


def timestamp(datestr):
    return int(pd.Timestamp(datestr).value / 10 ** 9)


@pytest.mark.parametrize('symbol, data_type, data, coerce_to', [
    ('TEST_COERCE', [('Epoch', 'i8'), ('Bid', 'f4'), ('Ask', 'f4')],
     [(timestamp('2020-01-01 00:00:00'), 1.0, 2.0),  # epoch, Ask, Bid
      (timestamp('2020-01-01 00:00:01'), 3.0, 4.0),
      (timestamp('2020-01-01 00:00:02'), 5.0, 6.0)],
     [('Epoch', 'i8'), ('Bid', 'f8'), ('Ask', 'f8')]  # coerce f8 to f4
     ),
])
def test_column_coerce(symbol, data_type, data, coerce_to):
    # ---- given ----
    tbk = "{}/1Sec/TICK".format(symbol)
    client.destroy(tbk)  # setup

    print(client.write(np.array(data, dtype=data_type), tbk, isvariablelength=False))

    # ---- when ----
    reply_before = client.query(pymkts.Params(symbol, '1Sec', 'TICK', limit=10))

    # write the same data but with different column dataType so that coerce_column happens
    ret = client.write(np.array(data, dtype=coerce_to), tbk, isvariablelength=False)

    reply_after = client.query(pymkts.Params(symbol, '1Sec', 'TICK', limit=10))

    # ---- then ----
    # no error (when there is no write error, json-rpc client returns "{'responses': None}" and gRPC client returns "")
    assert str(ret) in ["{'responses': None}", ""]

    # no data is updated
    assert (reply_before.first().df().values == reply_after.first().df().values).all()
