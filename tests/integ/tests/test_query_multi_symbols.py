"""
Integration Test for querying multiple symbols
"""
import os

import numpy as np
import pandas as pd
import pymarketstore as pymkts
import pytest

client = pymkts.Client(f"http://127.0.0.1:{os.getenv('MARKETSTORE_PORT', 5993)}/rpc",
                       grpc=(os.getenv("USE_GRPC", "false") == "true"))


@pytest.mark.parametrize('write_data, query_columns, want_err, want_data', [
    # even if the data format of 2 symbols are different,
    # querying succeeds if the common columns are specified
    ({
         'TEST1': {'data': [(pd.Timestamp('2017-01-01 00:00').value / 10 ** 9, 10.0)],
                   'dtype': [('Epoch', 'i8'), ('Ask', 'f4')],
                   'is_variable_length': False
                   },
         'TEST2': {'data': [(pd.Timestamp('2017-01-01 00:00').value / 10 ** 9, 20.0, 30.0)],
                   'dtype': [('Epoch', 'i8'), ('Ask', 'f4'), ('Bid', 'f4')],
                   'is_variable_length': False
                   },
     },
     # query_columns
     ['Ask'],
     # want_err
     False,
     # want_data
     {'TEST1': pd.DataFrame(data={'Ask': np.array([10.0], dtype='float32')},
                            index=pd.Series(['2017-01-01T00:00:00+00:00'],
                                            dtype='datetime64[ns, UTC]', name="Epoch")),
      'TEST2': pd.DataFrame(data={'Ask': np.array([20.0], dtype='float32')},
                            index=pd.Series(['2017-01-01T00:00:00+00:00'],
                                            dtype='datetime64[ns, UTC]', name="Epoch"))
      }
    ),
    # if common columns are not specified, query returns an error.
    ({
         'TEST1': {'data': [(pd.Timestamp('2017-01-01 00:00').value / 10 ** 9, 10.0)],
                   'dtype': [('Epoch', 'i8'), ('Ask', 'f4')],
                   'is_variable_length': False
                   },
         'TEST2': {'data': [(pd.Timestamp('2017-01-01 00:00').value / 10 ** 9, 20.0, 30.0)],
                   'dtype': [('Epoch', 'i8'), ('Ask', 'f4'), ('Bid', 'f4')],
                   'is_variable_length': False
                   },
     },
     # query_columns
     None,  # no columns specified
     # want_err
     True,
     # want_data
     None,
    ),
])
def test_query_multi_symbols(write_data, query_columns, want_err, want_data):
    # ---- given ----
    for symbol in write_data:
        tbk = "{}/1Sec/TICK".format(symbol)
        client.destroy(tbk)  # setup
        client.write(np.array(write_data[symbol]['data'], dtype=write_data[symbol]['dtype']), tbk,
                     isvariablelength=write_data[symbol]['is_variable_length'])

    # ---- when ----
    symbols = list(write_data.keys())
    if want_err:
        with pytest.raises(Exception) as excinfo:
            client.query(pymkts.Params(symbols, '1Sec', 'TICK', columns=query_columns))
        assert "symbols in a query must have the same data type or be filtered" in str(excinfo.value)
        return

    reply = client.query(pymkts.Params(symbols, '1Sec', 'TICK', columns=query_columns))

    # ---- then ----
    tbks = ["{}/1Sec/TICK".format(symbol) for symbol in symbols]
    for i in range(len(tbks)):
        want = want_data[symbols[i]]
        got = reply.all()[tbks[i]].df()
        assert got.equals(want)
