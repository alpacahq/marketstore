"""
Integration Test for Column Coercion feature
"""
import pytest
import os

import numpy as np
import pandas as pd

import pymarketstore as pymkts

# Constants
DATA_TYPE_TICK = [('Epoch', 'i8'), ('Bid', 'f4'), ('Ask', 'f4')]

client = pymkts.Client(f"http://127.0.0.1:{os.getenv('MARKETSTORE_PORT',5993)}/rpc",
                       grpc=(os.getenv("USE_GRPC", "false") == "true"))


def timestamp(datestr):
    return int(pd.Timestamp(datestr).value / 10 ** 9)


@pytest.mark.parametrize('symbol, data_type, data, coerce_to', [
    ('TEST_COERCE', [('Epoch', 'i8'), ('Bid', 'f4'), ('Ask', 'f4')],
     [(timestamp('2019-01-01 00:00:00'), 1.0, 2.0),  # epoch, Ask, Bid
      (timestamp('2019-01-01 00:00:01'), 3.0, 4.0),
      (timestamp('2019-12-31 23:59:59'), 5.0, 6.0)],
     [('Epoch', 'i8'), ('Bid', 'f4'), ('Ask', 'f8')] # coerce f8 to f4
     ),

])
def test_column_coerce(symbol, data_type, data, coerce_to):
    # ---- given ----
    tbk = "{}/1Sec/TICK".format(symbol)
    client.destroy(tbk)  # setup

    print(client.write(np.array(data, dtype=data_type), tbk, isvariablelength=False))

    # ---- when ----
    reply = client.query(pymkts.Params(symbol, '1Sec', 'TICK', limit=10))

    # coerce columns
    print(client.write(np.array(data, dtype=coerce_to), tbk, isvariablelength=False))

    # ---- then ----
    data_without_epochs = [record[1:] for record in data]
    print(reply.first().df().dtypes)
    # assert (reply.first().df().values == data_without_epochs).all()
