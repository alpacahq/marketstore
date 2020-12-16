"""
Integration Test for Create endpoint
"""
import os
from typing import List, Tuple

import pandas as pd
import pymarketstore as pymkts
import pytest

use_grpc = os.getenv("USE_GRPC", "false") == "true"
client = pymkts.Client(f"http://127.0.0.1:{os.getenv('MARKETSTORE_PORT',5993)}/rpc",
                       grpc=use_grpc)


def timestamp(datestr):
    return int(pd.Timestamp(datestr).value / 10 ** 9)


@pytest.mark.parametrize('symbol, dtype, is_variable_length', [
    ('CREATE1', [("Epoch", "i8"), ("Ask", "f4"), ("Bid", "f4")], False),
    ('CREATE2', [("Epoch", "i8"), ("Ask", "f4"), ("Bid", "f4")], True),  # isvariablelength=True
    # various data types
    ('CREATE3', [("BYTE", "i1"), ("INT16", "i2"), ("INT32", "i4"), ("INT64", "i8"), ("UINT8", "u1"),
                 ("UINT16", "u2"), ("UINT32", "u4"), ("UINT64", "u8"), ("FLOAT32", "f4"), ("FLOAT64", "f8")], False),
    ('CREATE4', [("BYTE", "i1"), ("INT16", "i2"), ("INT32", "i4"), ("INT64", "i8"), ("UINT8", "u1"),
                 ("UINT16", "u2"), ("UINT32", "u4"), ("UINT64", "u8"), ("FLOAT32", "f4"), ("FLOAT64", "f8")], True),
    ('CREATE4', [], False),  # empty datatype
    ('CREATE4', [], True)
])
def test_create(symbol: str, dtype: List[Tuple[str, str]], is_variable_length: bool):
    # ---- given ----
    tbk = "{}/1D/TICK".format(symbol)
    client.destroy(tbk)

    # ---- when ----
    create_resp = client.create(tbk=tbk, dtype=dtype, isvariablelength=is_variable_length)
    list_resp = client.list_symbols()

    # ---- then ----
    if use_grpc:
        print(create_resp.responses)
    else:
        assert create_resp["responses"][0]["error"] == ""

    # assert the symbol is created
    assert symbol in list_resp

    # ---- tearDown ----
    client.destroy(tbk)
