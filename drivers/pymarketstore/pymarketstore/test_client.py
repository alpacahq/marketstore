import pytest
from . import client as M
import numpy as np


def test_init():
    p = M.Params('TSLA', '1Min', 'OHLCV', 1500000000, 4294967296)
    tbk = "TSLA/1Min/OHLCV"
    assert p.tbk == tbk


def test_client_init():
    c = M.Client("http://127.0.0.1:5994/rpc")
    assert c.endpoint == "http://127.0.0.1:5994/rpc"
    assert isinstance(c.rpc, M.jsonrpc.MsgpackRpcClient)


@pytest.mark.skip
def test_get_column_data():
    c = M.Client()
    length = 2
    dt = [('Epoch', '<i8', (length,)), ('Open', '<f4', (length,)),
          ('High', '<f4', (length,)), ('Low', '<f4', (length,)),
          ('Close', '<f4', (length,)), ('Volume', '<i4', (length,))]
    dt = np.dtype(dt)
    arr = np.empty([1, ], dtype=dt)
    arr['Epoch'] = [2000000000, 2500000000]
    arr['Open'] = [152.369995, 152.339996]
    arr['High'] = [152.369995, 152.539993]
    arr['Low'] = [152.369995, 152.119995]
    arr['Close'] = [152.369995, 152.369995]
    arr['Volume'] = [215383, 466322]
    col_data = c.get_column_data(arr, length)
    test_data = [
        '\x00\x945w\x00\x00\x00\x00\x00\xf9\x02\x95\x00\x00\x00\x00',
        '\xb8^\x18C\nW\x18C',
        '\xb8^\x18C=\x8a\x18C',
        '\xb8^\x18C\xb8\x1e\x18C',
        '\xb8^\x18C\xb8^\x18C',
        'WI\x03\x00\x92\x1d\x07\x00']
    assert col_data == test_data


@pytest.mark.skip
def test_get_header():
    c = M.Client("127.0.0.1:5994")
    length = 2
    dt = [('Epoch', '<i8', (length,)), ('Open', '<f4', (length,)),
          ('High', '<f4', (length,)), ('Low', '<f4', (length,)),
          ('Close', '<f4', (length,)), ('Volume', '<i4', (length,))]
    dt = np.dtype(dt)
    arr = np.empty([1, ], dtype=dt)
    arr['Epoch'] = [2000000000, 2500000000]
    arr['Open'] = [152.369995, 152.339996]
    arr['High'] = [152.369995, 152.539993]
    arr['Low'] = [152.369995, 152.119995]
    arr['Close'] = [152.369995, 152.369995]
    arr['Volume'] = [215383, 466322]
    header = c.get_header(arr)
    test_header = "\x93NUMPY\x01\x00\xc6\x00{'descr': [('Epoch', '<i8', (2,)), ('Open', '<f4', (2,)), ('High', '<f4', (2,)), ('Low', '<f4', (2,)), ('Close', '<f4', (2,)), ('Volume', '<i4', (2,))], 'fortran_order': False, 'shape': (1,), }"  # noqa
    assert header == test_header


def test_build_query():
    c = M.Client("127.0.0.1:5994")
    p = M.Params('TSLA', '1Min', 'OHLCV', 1500000000, 4294967296)
    p2 = M.Params('FORD', '5Min', 'OHLCV', 1000000000, 4294967296)
    query_dict = c.build_query([p, p2])
    test_query_dict = {}
    test_lst = []
    param_dict1 = {
        'destination': 'TSLA/1Min/OHLCV',
        'epoch_start': 1500000000,
        'epoch_end': 4294967296
    }
    test_lst.append(param_dict1)
    param_dict2 = {
        'destination': 'FORD/5Min/OHLCV',
        'epoch_start': 1000000000,
        'epoch_end': 4294967296
    }
    test_lst.append(param_dict2)
    test_query_dict['requests'] = test_lst
    assert query_dict == test_query_dict
