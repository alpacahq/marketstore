import pytest
from client import *

class TestParams(object):
    def test_init(self):
        p = Params('TSLA', '1Min', 'OHLCV', 1500000000, 4294967296)
        time_bucket = "TSLA/1Min/OHLCV:Symbol/Timeframe/AttributeGroup"
        assert p.time_bucket == time_bucket
        with pytest.raises(ValueError):
            bad_p = Params('TSLA', '1Min', 'OHLCV', 1.1, 4324324)
        with pytest.raises(ValueError):
            bad_p = Params('TSLA', '1Min', 'OHLCV', 12341234, 4.4)
    def test_append_symbol(self):
        p = Params('TSLA', '1Min', 'OHLCV', 1500000000, 4294967296)
        p.append_symbol("NVDA")
        time_bucket = "TSLA,NVDA/1Min/OHLCV:Symbol/Timeframe/AttributeGroup"
        assert p.time_bucket == time_bucket

class TestClient(object):
    def test_init(self):
        c = Client("127.0.0.1:5994")
        assert c.base_url == "http://127.0.0.1:5994/rpc"
        assert c.rpc_client == jsonrpc.MsgpackRpcClient

    def test_get_column_data(self):
        c = Client("127.0.0.1:5994")
        length = 2
        dt = [('Epoch', '<i8', (length,)), ('Open', '<f4', (length,)), ('High', '<f4', (length,)), ('Low', '<f4', (length,)), ('Close', '<f4', (length,)), ('Volume', '<i4', (length,))]
        dt = np.dtype(dt)
        arr = np.empty([1, ], dtype=dt)
        arr['Epoch'] = [2000000000, 2500000000]
        arr['Open'] = [152.369995, 152.339996]
        arr['High'] = [152.369995, 152.539993]
        arr['Low'] = [152.369995, 152.119995]
        arr['Close'] = [152.369995, 152.369995]
        arr['Volume'] = [215383, 466322]
        col_data = c.get_column_data(arr, length)
        test_data = ['\x00\x945w\x00\x00\x00\x00\x00\xf9\x02\x95\x00\x00\x00\x00', '\xb8^\x18C\nW\x18C', '\xb8^\x18C=\x8a\x18C', '\xb8^\x18C\xb8\x1e\x18C', '\xb8^\x18C\xb8^\x18C', 'WI\x03\x00\x92\x1d\x07\x00']
        assert col_data == test_data

    def test_get_header(self):
        c = Client("127.0.0.1:5994")
        length = 2
        dt = [('Epoch', '<i8', (length,)), ('Open', '<f4', (length,)), ('High', '<f4', (length,)), ('Low', '<f4', (length,)), ('Close', '<f4', (length,)), ('Volume', '<i4', (length,))]
        dt = np.dtype(dt)
        arr = np.empty([1, ], dtype=dt)
        arr['Epoch'] = [2000000000, 2500000000]
        arr['Open'] = [152.369995, 152.339996]
        arr['High'] = [152.369995, 152.539993]
        arr['Low'] = [152.369995, 152.119995]
        arr['Close'] = [152.369995, 152.369995]
        arr['Volume'] = [215383, 466322]
        header = c.get_header(arr)
        test_header = "\x93NUMPY\x01\x00\xc6\x00{'descr': [('Epoch', '<i8', (2,)), ('Open', '<f4', (2,)), ('High', '<f4', (2,)), ('Low', '<f4', (2,)), ('Close', '<f4', (2,)), ('Volume', '<i4', (2,))], 'fortran_order': False, 'shape': (1,), }"
        assert header == test_header

    def test_to_dataframe(self):
        c = Client("127.0.0.1:5994")
        test_response = [{u'previoustime': {u'TSLA/1Min/OHLCV:Symbol/Timeframe/AttributeGroup': 1599999960}, u'version': u'dev', u'result': {u'lengths': {u'TSLA/1Min/OHLCV:Symbol/Timeframe/AttributeGroup': 7}, u'header': "\x93NUMPY\x01\x00\xc6\x00{'descr': [('Epoch', '<i8', (7,)), ('Open', '<f4', (7,)), ('High', '<f4', (7,)), ('Low', '<f4', (7,)), ('Close', '<f4', (7,)), ('Volume', '<i4', (7,)), ], 'fortran_order': False, 'shape': (1,),}    ", u'startindex': {u'TSLA/1Min/OHLCV:Symbol/Timeframe/AttributeGroup': 0}, u'columndata': ["X\xd3'a\x00\x00\x00\x00\xeci\xc0a\x00\x00\x00\x00\x18\xb5\x0cb\x00\x00\x00\x00x\x9f\rb\x00\x00\x00\x00\x80\x00Yb\x00\x00\x00\x00\xacK\xa5b\x00\x00\x00\x00\xd8\x96\xf1b\x00\x00\x00\x00", '\nW\x18C\nW\x18C\nW\x18C\nW\x18C\xb8^\x18C\nW\x18C\nW\x18C', '=\x8a\x18C=\x8a\x18C=\x8a\x18C=\x8a\x18C\xb8^\x18C=\x8a\x18C=\x8a\x18C', '\xb8\x1e\x18C\xb8\x1e\x18C\xb8\x1e\x18C\xb8\x1e\x18C\xb8^\x18C\xb8\x1e\x18C\xb8\x1e\x18C', '\xb8^\x18C\xb8^\x18C\xb8^\x18C\xb8^\x18C\xb8^\x18C\xb8^\x18C\xb8^\x18C', '\x92\x1d\x07\x00\x92\x1d\x07\x00\x92\x1d\x07\x00\x92\x1d\x07\x00WI\x03\x00\x92\x1d\x07\x00\x92\x1d\x07\x00'], u'columnnames': [u'Epoch', u'Open', u'High', u'Low', u'Close', u'Volume']}}]
        arr = {}
        idx = [1629999960, 1639999980, 1644999960, 1645059960, 1650000000, 1654999980, 1659999960]
        arr['Open'] = [152.339996, 152.339996, 152.339996, 152.339996, 152.369995, 152.339996, 152.339996]
        arr['High'] = [152.539993, 152.539993, 152.539993, 152.539993, 152.369995, 152.539993, 152.539993]
        arr['Low'] = [152.119995, 152.119995, 152.119995, 152.119995, 152.369995, 152.119995, 152.119995]
        arr['Close'] = [152.369995, 152.369995, 152.369995, 152.369995, 152.369995, 152.369995, 152.369995]
        arr['Volume'] = [466322, 466322, 466322, 466322, 215383, 466322, 466322]
        columnnames = ['Open', 'High', 'Low', 'Close', 'Volume']
        pd.set_option('precision', 6)
        test_df = pd.DataFrame(arr, columns=columnnames, index=idx)
        df_dict = c.to_dataframe(test_response)
        df = df_dict['TSLA/1Min/OHLCV']
        
        assert df['Open'].round(6).tolist() == test_df['Open'].round(6).tolist()
        assert df['High'].round(6).tolist() == test_df['High'].round(6).tolist()
        assert df['Low'].round(6).tolist() == test_df['Low'].round(6).tolist()
        assert df['Close'].round(6).tolist() == test_df['Close'].round(6).tolist()
        assert df['Volume'].tolist() == test_df['Volume'].tolist()
        assert df.index.tolist() == test_df.index.tolist()

    def test_decode_binary(self):
        c = Client("127.0.0.1:5994")
        length = 7
        dt = [('Epoch', '<i8', (length,)), ('Open', '<f4', (length,)), ('High', '<f4', (length,)), ('Low', '<f4', (length,)), ('Close', '<f4', (length,)), ('Volume', '<i4', (length,))]
        dt = np.dtype(dt)
        col_data = ["X\xd3'a\x00\x00\x00\x00\xeci\xc0a\x00\x00\x00\x00\x18\xb5\x0cb\x00\x00\x00\x00x\x9f\rb\x00\x00\x00\x00\x80\x00Yb\x00\x00\x00\x00\xacK\xa5b\x00\x00\x00\x00\xd8\x96\xf1b\x00\x00\x00\x00", '\nW\x18C\nW\x18C\nW\x18C\nW\x18C\xb8^\x18C\nW\x18C\nW\x18C', '=\x8a\x18C=\x8a\x18C=\x8a\x18C=\x8a\x18C\xb8^\x18C=\x8a\x18C=\x8a\x18C', '\xb8\x1e\x18C\xb8\x1e\x18C\xb8\x1e\x18C\xb8\x1e\x18C\xb8^\x18C\xb8\x1e\x18C\xb8\x1e\x18C', '\xb8^\x18C\xb8^\x18C\xb8^\x18C\xb8^\x18C\xb8^\x18C\xb8^\x18C\xb8^\x18C', '\x92\x1d\x07\x00\x92\x1d\x07\x00\x92\x1d\x07\x00\x92\x1d\x07\x00WI\x03\x00\x92\x1d\x07\x00\x92\x1d\x07\x00']
        arr = c.decode_binary(dt, col_data, length)
        columnnames = ['Epoch', 'Open', 'High', 'Low', 'Close', 'Volume']
        test_arr = {}
        test_arr['Epoch'] = (1629999960, 1639999980, 1644999960, 1645059960, 1650000000, 1654999980, 1659999960)
        test_arr['Open'] = (152.339996, 152.339996, 152.339996, 152.339996, 152.369995, 152.339996, 152.339996)
        test_arr['High'] = (152.539993, 152.539993, 152.539993, 152.539993, 152.369995, 152.539993, 152.539993)
        test_arr['Low'] = (152.119995, 152.119995, 152.119995, 152.119995, 152.369995, 152.119995, 152.119995)
        test_arr['Close'] = (152.369995, 152.369995, 152.369995, 152.369995, 152.369995, 152.369995, 152.369995)
        test_arr['Volume'] = (466322, 466322, 466322, 466322, 215383, 466322, 466322)

        for col in columnnames:
            rounded_test_arr = [ '%.6f' % elem for elem in test_arr[col] ]
            rounded_arr = [ '%.6f' % elem for elem in arr[col] ]
            assert rounded_test_arr == rounded_arr

    def test_header_to_dt(self):
        c = Client("127.0.0.1:5994")
        header = "\x93NUMPY\x01\x00\xc6\x00{'descr': [('Epoch', '<i8', (7,)), ('Open', '<f4', (7,)), ('High', '<f4', (7,)), ('Low', '<f4', (7,)), ('Close', '<f4', (7,)), ('Volume', '<i4', (7,)), ], 'fortran_order': False, 'shape': (1,),}    "
        header = c.header_to_dt(header)
        test_header = [('Epoch', '<i8', (7,)), ('Open', '<f4', (7,)), ('High', '<f4', (7,)), ('Low', '<f4', (7,)), ('Close', '<f4', (7,)), ('Volume', '<i4', (7,))]
        assert header == test_header

    def test_get_total_length(self):
        c = Client("127.0.0.1:5994")
        lengths = {u'TSLA/1Min/OHLCV:Symbol/Timeframe/AttributeGroup': 7, u'NVDA/1Min/OHLCV:Symbol/Timeframe/AttributeGroup': 7}
        total = c.get_total_length(lengths)
        assert total == 14

    def test_build_query(self):
        c = Client("127.0.0.1:5994")
        p = Params('TSLA', '1Min', 'OHLCV', 1500000000, 4294967296)
        p2 = Params('FORD', '5Min', 'OHLCV', 1000000000, 4294967296)
        query_dict = c.build_query([p, p2])
        test_query_dict = {}
        test_lst = []
        param_dict1 = {
            'destination': {'key':'TSLA/1Min/OHLCV:Symbol/Timeframe/AttributeGroup'},
            'timestart': 1500000000,
            'timeend': 4294967296
        }
        test_lst.append(param_dict1)
        param_dict2 = {
            'destination': {'key':'FORD/5Min/OHLCV:Symbol/Timeframe/AttributeGroup'},
            'timestart': 1000000000,
            'timeend': 4294967296
        }
        test_lst.append(param_dict2)
        test_query_dict['requests'] = test_lst
        assert query_dict == test_query_dict
