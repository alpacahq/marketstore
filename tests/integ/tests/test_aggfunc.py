import pytest
import os

import numpy as np
import pandas as pd

import pymarketstore as pymkts

client = pymkts.Client(f"http://127.0.0.1:{os.getenv('MARKETSTORE_PORT', 5993)}/rpc",
                       grpc=(os.getenv("USE_GRPC", "false") == "true"))


def timestamp(datestr):
    return int(pd.Timestamp(datestr).value / 10 ** 9)


symbol = "TEST_AGG"
data_type = [('Epoch', 'i8'), ('Example', 'f4'), ('Example2', 'f4')]
data = [(timestamp('2020-01-01 00:00:00'), 2.0, 20.0),  # Epoch, Example, Example2
        (timestamp('2020-01-01 00:00:01'), 4.0, 40.0),
        (timestamp('2020-01-01 00:00:02'), 6.0, 60.0),
        (timestamp('2020-01-01 00:00:03'), 8.0, 80.0),
        ]


@pytest.mark.parametrize('aggfunc, limit, limit_from_start, exp_col_name, exp_value', [
    # AVG
    (["AVG (Example)"], 2, True, "Avg", 3.0),  # (2.0+4.0)/2 = 3.0
    (["AVG (Example)"], 3, True, "Avg", 4.0),  # (2.0+4.0+6.0)/3 = 4.0
    (["AVG (Example)"], 2, False, "Avg", 7.0),  # (8.0+6.0)/2 = 7.0
    (["AVG (Example)"], 200, True, "Avg", 5.0),  # (2.0+4.0+6.0+8.0)/4 = 5.0
    (["AVG (Example2)"], 2, True, "Avg", 30.0),  # (20.0+40.0)/2 = 30.0
    # MAX
    (["MAX (Example)"], 2, True, "Max", 4.0),
    (["MAX (Example)"], 3, True, "Max", 6.0),
    (["MAX (Example)"], 2, False, "Max", 8.0),
    (["MAX (Example)"], 200, True, "Max", 8.0),
    (["MAX (Example2)"], 2, True, "Max", 40.0),
    # MIN
    (["MIN (Example)"], 2, True, "Min", 2.0),
    (["MIN (Example)"], 3, True, "Min", 2.0),
    (["MIN (Example)"], 2, False, "Min", 6.0),
    (["MIN (Example)"], 200, True, "Min", 2.0),
    (["MIN (Example2)"], 2, True, "Min", 20.0),
    # Count
    (["COUNT (Example)"], 2, True, "Count", 2),
    (["COUNT (Example)"], 3, True, "Count", 3),
    (["COUNT (Example)"], 2, False, "Count", 2),
    (["COUNT (Example)"], len(data) + 100, True, "Count", len(data)),
    (["COUNT (Example2)"], 2, True, "Count", 2),
])
def test_agg(aggfunc, limit, limit_from_start, exp_col_name, exp_value):
    # ---- given ----
    tbk = "{}/1Sec/TICK".format(symbol)
    client.destroy(tbk)  # setup

    client.write(np.array(data, dtype=data_type), tbk, isvariablelength=False)

    # ---- when ----
    agg_reply = client.query(pymkts.Params(symbol, '1Sec', 'TICK', limit=limit, limit_from_start=limit_from_start,
                                           functions=aggfunc))

    # ---- then ----
    assert agg_reply.first().df()[exp_col_name][0] == exp_value


gap_data_type = [('Epoch', 'i8'), ('Example', 'f4')]
gap_data = [(timestamp('2020-01-01 00:00:00'), 1.0),  # Epoch, Example
            (timestamp('2020-01-01 00:00:10'), 1.0),  # 00:00:00 -> 00:00:10  Gap=10sec
            (timestamp('2020-01-01 00:00:30'), 1.0),  # 00:00:10 -> 00:00:30  Gap=20sec
            (timestamp('2020-01-01 00:01:00'), 1.0),  # 00:00:30 -> 00:01:00  Gap=30sec
            ]


@pytest.mark.parametrize('aggfunc, exp_value_len', [
    # GAP
    (["GAP ('1Sec')"], 3),
    (["GAP ('10Sec')"], 2),
    (["GAP ('20Sec')"], 1),
    (["GAP ('30Sec')"], 0),
])
def test_gap(aggfunc, exp_value_len):
    # ---- given ----
    tbk = "GAPTEST/1Sec/TICK".format(symbol)
    client.destroy(tbk)  # setup

    client.write(np.array(gap_data, dtype=gap_data_type), tbk, isvariablelength=False)

    # ---- when ----
    agg_reply = client.query(pymkts.Params("GAPTEST", '1Sec', 'TICK', limit=None, limit_from_start=True,
                                           functions=aggfunc))

    # ---- then ----
    print("\n")
    print(agg_reply.first().df().values)
    assert len(agg_reply.first().df()) == exp_value_len
