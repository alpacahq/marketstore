"""
Integration Test for anomaly detector
"""
import pytest
import os

import numpy as np
import pandas as pd
from pandas.testing import assert_frame_equal

import pymarketstore as pymkts

# Constants
DATA_TYPE_TICK = [('Epoch', 'i8'), ('Bid', 'f4'), ('Ask', 'f4'), ('Nanoseconds', 'i4')]
DATA_TYPE_CANDLE = [('Epoch', 'i8'), ('Open', 'f8'), ('High', 'f8'), ('Low', 'f8'), ('Close', 'f8'), ('Volume', 'f8')]
MARKETSTORE_HOST = "localhost"
MARKETSTORE_PORT = 5993

client = pymkts.Client(f"http://127.0.0.1:{os.getenv('MARKETSTORE_PORT',5993)}/rpc",
                       grpc=(os.getenv("USE_GRPC", "false") == "true"))


def timestamp(datestr):
    return int(pd.Timestamp(datestr).value / 10 ** 9)


@pytest.mark.parametrize('symbol, columns, detection_type, threshold, data, expected_df', [
    ('AT_SINGLE_COL_FIXED', 'Ask', 'fixed_pct', '0.045',
     [(timestamp('2019-01-01 04:19:00'), 15, 11,   0.01),  # epoch, bid, ask, nanosecond
      (timestamp('2019-01-01 04:19:01'), 20, 11.5, 0.02),
      (timestamp('2019-01-02 05:59:59'), 30, 11.6, 0.03)],
     pd.DataFrame(index=[pd.Timestamp('2019-01-01 04:19:01', tz='UTC')],
                  data=np.array([1], dtype='uint64'), columns=['ColumnsBitmap']).rename_axis('Epoch')),
    ('AT_MULTI_COL_FIXED', 'Bid,Ask', 'fixed_pct', '0.045',
     [(timestamp('2019-01-01 04:19:00'), 15, 11,   0.01),
      (timestamp('2019-01-01 04:19:01'), 20, 11.5, 0.02),
      (timestamp('2019-01-02 05:59:59'), 30, 11.6, 0.03)],
     pd.DataFrame(index=[pd.Timestamp('2019-01-01 04:19:01', tz='UTC'),
                         pd.Timestamp('2019-01-02 05:59:59', tz='UTC')],
                  data=np.array([3, 1], dtype='uint64'), columns=['ColumnsBitmap']).rename_axis('Epoch')),
    ('AT_SINGLE_COL_ZSCORE', 'Bid', 'z_score', '1.0',
     [(timestamp('2019-01-01 04:19:00'), 10.1, 11,   0.01),
      (timestamp('2019-01-01 04:19:01'), 10.2, 11.5, 0.02),
      (timestamp('2019-01-01 04:19:03'), 10.1, 11.5, 0.03),
      (timestamp('2019-01-01 04:19:04'), 10.3, 11.5, 0.04),
      (timestamp('2019-01-01 04:19:05'), 10.2, 11.5, 0.05),
      (timestamp('2019-01-01 04:19:06'), 10.2, 11.5, 0.06),
      (timestamp('2019-01-02 05:59:59'), 100.1, 11.6, 0.07)],
     pd.DataFrame(index=[pd.Timestamp('2019-01-02 05:59:59', tz='UTC')],
                  data=np.array([1], dtype='uint64'), columns=['ColumnsBitmap']).rename_axis('Epoch')),
    ('AT_MULTI_COL_ZSCORE', 'Bid,Ask', 'z_score', '1.0',
     [(timestamp('2019-01-01 04:19:00'), 10.1, 11,   0.01),
      (timestamp('2019-01-01 04:19:01'), 10.2, 11.5, 0.02),
      (timestamp('2019-01-01 04:19:03'), 10.1, 0.0015, 0.03),
      (timestamp('2019-01-01 04:19:04'), 10.3, 11.5, 0.04),
      (timestamp('2019-01-01 04:19:05'), 10.2, 11.5, 0.05),
      (timestamp('2019-01-01 04:19:06'), 10.2, 11.5, 0.06),
      (timestamp('2019-01-02 05:59:59'), 100.1, 11.6, 0.07)],
     pd.DataFrame(index=[pd.Timestamp('2019-01-01 04:19:03', tz='UTC'),
                         pd.Timestamp('2019-01-02 05:59:59', tz='UTC')],
                  data=np.array([2,1], dtype='uint64'), columns=['ColumnsBitmap']).rename_axis('Epoch')),
])
def test_anomaly_one_symbol(symbol, columns, detection_type, threshold, data, expected_df):
    # ---- given ----
    tbk = "{}/1Sec/TICK".format(symbol)
    client.destroy(tbk)
    client.write(np.array(data, dtype=DATA_TYPE_TICK), tbk, isvariablelength=True)

    # ---- when ----
    params = pymkts.Params(symbol, '1Sec', 'TICK')
    params.functions = [f"anomaly('{columns}', '{detection_type}', '{threshold}')"]
    reply = client.query(params)

    # ---- then ----
    actual_df = reply.first().df()
    assert_frame_equal(actual_df, expected_df)
