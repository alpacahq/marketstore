"""
Integration Test for String type column
"""
import pytest
import os

import numpy as np
import pandas as pd

import pymarketstore as pymkts


client = pymkts.Client(f"http://127.0.0.1:{os.getenv('MARKETSTORE_PORT', 5993)}/rpc",
                       grpc=(os.getenv("USE_GRPC", "false") == "true"))

def timestamp(datestr):
    return int(pd.Timestamp(datestr).value / 10 ** 9)


string_column_name = "Memo"
dtype = [('Epoch', 'i8'), (string_column_name, 'U16')]


@pytest.mark.parametrize('tbk, record_array, is_variable_length', [
    # single value
    ('STR1/1D/TICK',
     np.array([(timestamp('2021-02-01 00:00:00'), "hello")], dtype=dtype),
     False),
    # is_variable_length = True
    ('STR1/1D/TICK',
     np.array([(timestamp('2021-02-01 00:00:00'), "world")], dtype=dtype),
     True),
    # multi values
    ('STR1/1D/TICK',
     np.array([(timestamp('2019-02-01 00:00:00'), "fizz"),
               (timestamp('2019-02-02 00:00:00'), "buzz")], dtype=dtype),
     False),
    # multi values, is_variable_length = True
    ('STR1/1D/TICK',
     np.array([(timestamp('2019-02-01 00:00:00'), "fizz"),
               (timestamp('2019-02-02 00:00:00'), "buzz")], dtype=dtype),
     True),
    # multi-byte characters
    ('STR1/1D/TICK',
     np.array([(timestamp('2021-02-01 00:00:00'), "こんにちは世界")], dtype=dtype),
     False),
    # 4-byte character '🍺'
    ('STR1/1D/TICK',
     np.array([(timestamp('2021-02-01 00:00:00'), "🍺🍺🍺")], dtype=dtype),
     True),
])
def test_write_query(tbk: str, record_array: np.array,
                     is_variable_length: bool):
    # --- given ---
    client.destroy(tbk)

    # --- when ---
    client.write(recarray=record_array, tbk=tbk, isvariablelength=False)
    symbol, timeframe, attribute_group = tbk.split("/")
    param = pymkts.Params(symbol, timeframe, attribute_group)
    reply = client.query(param)

    # --- then ---
    # written strings equal to the string column returned from query
    string_column = reply.first().df()[string_column_name]
    data_length = len(string_column)
    for i in range(data_length):
        str_value = record_array[i][1]
        assert str_value == string_column[i]


def test_too_long_string():
    # --- given ---
    tbk = "STR2/1D/TICK"
    client.destroy(tbk)

    recarray = np.array([(timestamp('2019-05-01 00:00:00'), "this_is_longer_than_16_characters")], dtype=dtype)

    # --- when ---
    rep = client.write(recarray=recarray, tbk=tbk, isvariablelength=False)
    symbol, timeframe, attribute_group = tbk.split("/")
    param = pymkts.Params(symbol, timeframe, attribute_group)
    reply = client.query(param)

    # --- then ---
    print(reply.first().df())
