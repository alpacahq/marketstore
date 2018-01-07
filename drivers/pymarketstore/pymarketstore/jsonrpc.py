import json

import msgpack
import requests


class JsonRpcServer(object):

    def __init__(self, **kwargs):
        self._methods = kwargs

    def handle(self, req):
        assert req['jsonrpc'] == '2.0'
        try:
            result = self._methods[req['method']](**req['params'])
            return dict(
                jsonrpc='2.0',
                id=req['id'],
                result=result,
            )
        except Exception:
            import traceback
            lines = traceback.format_exc().splitlines()
            return dict(
                jsonrpc='2.0',
                id=req['id'],
                error=dict(
                    code=-32603,
                    message=lines[-1],
                    data='\n'.join(lines[1:-2]),
                ),
            )


class JsonRpcClient(object):

    encoder = json
    mimetype = "application/json"

    def __init__(self, endpoint=None, cookies=None):
        self._id = 1
        self._endpoint = endpoint
        self._cookies = cookies
        self._session = requests.Session()

    def __getattr__(self, method):
        assert self._endpoint is not None

        def call(**kwargs):
            if self._cookies:
                return self._session.post(
                    self._endpoint,
                    data=self.encoder.dumps(self.request(method, **kwargs)),
                    headers={"Content-Type": self.mimetype},
                    cookies=self._cookies)
            else:
                return self._session.post(
                    self._endpoint,
                    data=self.encoder.dumps(self.request(method, **kwargs)),
                    headers={"Content-Type": self.mimetype})
        return call

    def call(self, method, **kwargs):
        return getattr(self, method)(**kwargs)

    def request(self, method, **kwargs):
        req = dict(
            method=method,
            id=str(self._id),
            jsonrpc='2.0',
            params=kwargs,
        )
        return req

    @staticmethod
    def response(resp):
        if 'result' in resp:
            return resp['result']

        if 'error' not in resp:
            raise Exception('invalid JSON-RPC protocol: missing error')

        raise Exception('{}:\n{}'.format(
            resp['error']['message'],
            str(resp['error']['data'])))


class MsgpackRpcServer(JsonRpcServer):
    pass


class MsgpackRpcClient(JsonRpcClient):
    encoder = msgpack
    mimetype = "application/x-msgpack"


class Serializable(object):
    '''
    A class to be mixed-in to implement json/other serialization.
    '''

    '''class(es) to describe how to decode fields'''
    _transcoders = dict()

    '''list of names for fields to be encoded/decoded'''
    _fileds = list()

    def _serialize_one(self, val, encoder):
        if encoder is not None:
            return encoder.encode(val)
        return val

    def _to_primitive(self):
        if isinstance(self, list):
            ary = list()
            for val in self:
                ary.append(self._serialize_one(val, self._transcoders))
            return ary
        else:
            obj = dict()
            for name in self._fields:
                if not hasattr(self, name):
                    continue
                val = self.__getattribute__(name)
                obj[name] = self._serialize_one(
                    val, self._transcoders.get(name))

            return obj

    @classmethod
    def encode(cls, val):
        return val._to_primitive()

    def to_json(self):
        return json.dumps(self._to_primitive())

    @classmethod
    def _deserialize_one(cls, val, decoder):
        if decoder is not None:
            return decoder.decode(val)
        return val

    @classmethod
    def _from_primitive(cls, src):
        if issubclass(cls, list):
            if not isinstance(src, list):
                raise ValueError('unexpected non-list object')
            ary = cls()
            for val in src:
                ary.append(cls._deserialize_one(val, cls._transcoders))
            return ary
        else:
            obj = dict()
            for name in cls._fields:
                if name in src:
                    val = src[name]
                    obj[name] = cls._deserialize_one(
                        val, cls._transcoders.get(name))
                else:
                    obj[name] = None
            return cls(**obj)

    @classmethod
    def decode(cls, src):
        return cls._from_primitive(src)

    @classmethod
    def from_json(cls, ser_str):
        return cls._from_primitive(json.loads(ser_str))
