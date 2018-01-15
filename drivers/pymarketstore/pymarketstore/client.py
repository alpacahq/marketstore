from tempfile import TemporaryFile
import numpy as np
import pandas as pd
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


def get_timestamp(value):
    if value is None:
        return None
    if isinstance(value, (int, np.integer)):
        return pd.Timestamp(value, unit='s')
    return pd.Timestamp(value)


class Params(object):

    def __init__(self, symbols, timeframe, attrgroup,
                 start=None, end=None,
                 limit=None, limit_from_start=None):
        if not isiterable(symbols):
            symbols = [symbols]
        self.tbk = ','.join(symbols) + "/" + timeframe + "/" + attrgroup
        self.key_category = None  # server default
        self.start = get_timestamp(start)
        self.end = get_timestamp(end)
        self.limit = limit
        self.limit_from_start = limit_from_start
        self.functions = None

    def set(self, key, val):
        if not hasattr(self, key):
            raise AttributeError()
        if key in ('start', 'end'):
            setattr(self, key, get_timestamp(val))
        else:
            setattr(self, key, val)
        return self

    def __repr__(self):
        content = (f'tbk={self.tbk}, start={self.start}, end={self.end}, ' +
                   f'limit={self.limit}, ' +
                   f'limit_from_start={self.limit_from_start}')
        return f'Params({content})'



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
            req = {
                'destination': param.tbk,
            }
            if param.key_category is not None:
                req['key_category'] = param.key_category
            if param.start is not None:
                req['epoch_start'] = int(param.start.value / (10 ** 9))
            if param.end is not None:
                req['epoch_end'] = int(param.end.value / (10 ** 9))
            if param.limit is not None:
                req['limit_record_count'] = int(param.limit)
            if param.limit_from_start is not None:
                req['limit_from_start'] = bool(param.limit_from_start)
            if param.functions is not None:
                req['functions'] = param.functions
            reqs.append(req)
        return {
            'requests': reqs,
        }

    def __repr__(self):
        return f'Client("{self.endpoint}")'
