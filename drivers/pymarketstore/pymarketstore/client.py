from tempfile import TemporaryFile
import numpy as np
import six
import requests
import msgpack
import struct
import logging

from . import jsonrpc
from .results import QueryReply

logger = logging.getLogger(__name__)


data_type_conv = {
    '<f4': 'f',
    '<f8': 'd',
    '<i4': 'i',
    '<i8': 'q',
}


def isiterable(something):
    return isinstance(something, (list, tuple, set))


def get_rpc_client(codec='msgpack'):
    if codec == 'msgpack':
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

    def __init__(self, endpoint='http://localhost:5993/rpc'):
        self.endpoint = endpoint
        rpc_client = get_rpc_client('msgpack')
        self.rpc = rpc_client(self.endpoint)

    def _request(self, method, **query):
        try:
            resp = self.rpc.call(method, **query)
            resp.raise_for_status()
            rpc_reply = self.rpc.codec.loads(resp.content, encoding='utf-8')
            return self.rpc.response(rpc_reply)
        except requests.exceptions.HTTPError as exc:
            logger.exception(exc)
            raise

    def query(self, params):
        single_result = False
        if not isiterable(params):
            params = [params]
            single_result = True
        query = self.build_query(params)
        reply = self._request('DataService.Query', **query)
        return QueryReply(reply)

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
        try:
            reply = self.rpc.call("DataService.Write", **writer)
        except requests.exceptions.ConnectionError:
            raise requests.exceptions.ConnectionError("Could not contact server")
        reply_obj = self.rpc.codec.loads(reply.content, encoding='utf-8')
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

    def build_query(self, params):
        reqs = []
        if not isiterable(params):
            params = [params]
        for param in params:
            reqs.append({
                'destination': {'key': param.time_bucket},
                'timestart': param.start,
                'timeend': param.end,
            })
        return {
            'requests': reqs,
        }
