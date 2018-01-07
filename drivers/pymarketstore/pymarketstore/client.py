from ast import literal_eval
from tempfile import TemporaryFile
import numpy.lib.format as npformat
from . import jsonrpc
import numpy as np
import pandas as pd
from six import BytesIO
import requests
import msgpack
import base64
import struct
import json

def get_rpc_client(encoder='msgpack'):
    if encoder == 'msgpack':
        return jsonrpc.MsgpackRpcClient
    return jsonrpc.JsonRpcClient

class Params(object):

    time_bucket = None
    start = None
    end = None

    def __init__(self, symbol, time_frame, attr_group, start, end):
        self.time_bucket = symbol + "/" + time_frame + "/" + attr_group + ":"
        self.time_bucket += "Symbol/Timeframe/AttributeGroup"
        if isinstance(start, int) and isinstance(end, int):
            self.start = start
            self.end = end
        else:
            raise ValueError("start and end times must be integers")

    def append_symbol(self, symbol):
        orig_symbols = self.time_bucket.split('/', 1)[0]
        rest = '/' + self.time_bucket.split('/', 1)[1]
        new_symbols = orig_symbols + ',' + symbol
        self.time_bucket = new_symbols + rest

class Client(object):

    base_url = None
    rpc_client = None
    rpc = None

    def __init__(self, connect_url):
        if len(connect_url.split(":")) != 2:
            raise ValueError("URL format: <hostname>:<port>")
        self.base_url = "http://" + connect_url + "/rpc"
        self.rpc_client = get_rpc_client("msgpack")
        self.rpc = self.rpc_client(self.base_url)

    def query(self, params_obj_lst):
        if not isinstance(params_obj_lst, list):
            raise ValueError("intput must be a list of parameter objects")
        to_query = self.build_query(params_obj_lst)
        try:
            reply = self.rpc.call("DataService.Query", **to_query)
        except requests.exceptions.ConnectionError:
            raise requests.exceptions.ConnectionError("Could not contact server")
        reply_obj = self.rpc.encoder.loads(reply.content, encoding='utf-8')
        resp = self.rpc.response(reply_obj)
        data = resp['responses']
        df = self.to_dataframe(data)
        return df

    def write(self, np_arr, tbk):
        data = {}
        data['header'] = self.get_header(np_arr)
        data['columnnames'] = np_arr.dtype.names
        data['columndata'] = self.get_column_data(np_arr, len(np_arr[0][0]))
        data['length'] = len(np_arr[0][0])
        data['startindex'] = {tbk:0}
        data['lengths'] = {tbk:len(np_arr[0][0])}
        write_request = {}
        write_request['data'] = data
        write_request['isvariablelength'] = False
        writer = {}
        writer['requests'] = [write_request]
        dt = np.dtype(self.header_to_dt(data['header']))
        arr = self.decode_binary(dt, data['columndata'], 2)
        try:
            reply = self.rpc.call("DataService.Write", **writer)
        except requests.exceptions.ConnectionError:
            raise requests.exceptions.ConnectionError("Could not contact server")
        reply_obj = self.rpc.encoder.loads(reply.content, encoding='utf-8')
        resp = self.rpc.response(reply_obj)
        return resp

    def get_column_data(self, data, length):
        col_data = []
        prev_index = 0
        for idx, name in enumerate(data.dtype.names):
            buf = ''
            record_type = data.dtype[idx]
            record_type = str(record_type)[2:5]
            typestr = "<{}".format(data_type_conv[record_type])
            for i in data[name][0]:
                buf += struct.pack(typestr, i)
            col_data.append(buf)
        return col_data

    def get_header(self, data):
        tmp_file = TemporaryFile()
        np.save(tmp_file, data)
        tmp_file.seek(0)
        header = tmp_file.read().split('}')[0] + '}'
        tmp_file.close()
        return header

    def to_dataframe(self, response_lst):
        df_dict = {}
        for data in response_lst:
            if data['result'] == None:
                df = pd.DataFrame()
                if data['previoustime'].keys()[0]:
                    key = data['previoustime'].keys()[0]
                    key = key.split(':')[0]
                else:
                    key = "unknown"
                df_dict[key] = df
                continue
            data = data['result']
            dt = np.dtype(self.header_to_dt(data['header']))
            length = self.get_total_length(data['lengths'])
            cols = data['columnnames']
            arr = self.decode_binary(dt, data['columndata'], length)
            idx = arr[data['columnnames'][0]]
            df = pd.DataFrame(arr, columns=data['columnnames'], index=idx)
            df = df.drop(data['columnnames'][0], 1)
            if len(data['lengths'].keys()) > 1:
                for key, start_idx in data['startindex'].iteritems():
                    length = data['lengths'][key]
                    key = key.split(':')[0]
                    try:
                        df_dict[key] = df[start_idx:(start_idx + length)]
                    except:
                        df_dict[key] = df[start_idx:]
            else:
                key = data['lengths'].keys()[0]
                key = key.split(':')[0]
                df_dict[key] = df
        return df_dict

    def decode_binary(self, dt, columndata, length):
        arr = {}
        prev_index = 0
        for idx, name in enumerate(dt.names):
            record_type = dt[idx]
            record_type = str(record_type)[2:5]
            typestr = "<{}{}".format(length, data_type_conv[record_type])
            buf = BytesIO(columndata[idx]).read()
            buf = struct.unpack(typestr, buf)
            arr[name] = buf
        return arr

    def header_to_dt(self, header):
        dt_begin = header.index('descr') + 8
        dt_end = header.index(", 'fortran_order'")
        dt_str = header[dt_begin:dt_end]
        return literal_eval(dt_str)

    def get_total_length(self, lengths):
        total = 0
        for key, num in lengths.iteritems():
            total += num
        return total

    def build_query(self, params_obj_lst):
        query_dict = {}
        lst_builder = []
        for param in params_obj_lst:
            param_dict = {}
            param_dict['destination'] = {"key": param.time_bucket}
            param_dict['timestart'] = param.start
            param_dict['timeend'] = param.end
            lst_builder.append(param_dict)
        query_dict['requests'] = lst_builder
        return query_dict

data_type_conv = {
    '<f4': 'f',
    '<f8': 'd',
    '<i4': 'i',
    '<i8': 'q',
}
