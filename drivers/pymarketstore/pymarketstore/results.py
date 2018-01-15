import numpy as np
import pandas as pd
import six
import struct

def f(a):
    a.names = [n.lower() for n in a.names]

def decode(packed):
    dt = np.dtype([
        (colname, coltype)
        for colname, coltype in zip(packed['names'], packed['types'])
    ])
    array = np.empty((packed['length'],), dtype=dt)
    for idx, name in enumerate(dt.names):
        array[name] = np.frombuffer(packed['data'][idx], dtype=dt[idx])
    return array


def decode_responses(responses):
    results = []
    for response in responses:
        packed = response['result']
        array_dict = {}
        array = decode(packed)
        for tbk, start_idx in six.iteritems(packed['startindex']):
            length = packed['lengths'][tbk]
            key = tbk.split(':')[0]
            array_dict[key] = array[start_idx:start_idx+length]
        results.append(array_dict)
    return results


class DataSet(object):

    def __init__(self, array, key, reply):
        self.array = array
        self.key = key
        self.reply = reply

    @property
    def timezone(self):
        return self.reply['timezone']

    @property
    def symbol(self):
        return self.key.split('/')[0]

    @property
    def timeframe(self):
        return self.key.split('/')[1]

    @property
    def attribute_group(self):
        return self.key.split('/')[2]

    def df(self):
        idxname = self.array.dtype.names[0]
        df = pd.DataFrame(self.array).set_index(idxname)
        index = pd.to_datetime(df.index, unit='s', utc=True)
        tz = self.timezone
        if tz.lower() != 'utc':
            index = index.tz_convert(tz)
        df.index = index
        return df

    def __repr__(self):
        a = self.array
        return f'DataSet(key={self.key}, shape={a.shape}, dtype={a.dtype})'


class QueryResult(object):

    def __init__(self, result, reply):
        self.result = {
            key: DataSet(value, key, reply)
            for key, value in six.iteritems(result)
            }
        self.reply = reply

    @property
    def timezone(self):
        return self.reply['timezone']

    def keys(self):
        return list(self.result.keys())

    def first(self):
        return self.result[self.keys()[0]]

    def all(self):
        return self.result

    def __repr__(self):
        content = '\n'.join([
            str(ds) for _, ds in six.iteritems(self.result)
        ])
        return f'QueryResult({content})'


class QueryReply(object):

    def __init__(self, reply):
        results = decode_responses(reply['responses'])
        self.results = [QueryResult(result, reply) for result in results]
        self.reply = reply

    @property
    def timezone(self):
        return self.reply['timezone']

    def first(self):
        return self.results[0].first()

    def all(self):
        datasets = {}
        for result in self.results:
            datasets.update(result.all())
        return datasets

    def keys(self):
        keys = []
        for result in self.results:
            keys += result.keys()
        return keys

    def get_catkeys(self, catnum):
        ret = set()
        for key in self.keys():
            elems = key.split('/')
            ret.add(elems[catnum])
        return list(ret)

    @property
    def symbols(self):
        return self.get_catkeys(0)

    @property
    def timeframes(self):
        return self.get_catkeys(1)

    def by_symbols(self):
        datasets = self.all()
        ret = {}
        for key, dataset in six.iteritems(datasets):
            symbol = key.split('/')[0]
            ret[symbol] = dataset
        return ret

    def __repr__(self):
        content = '\n'.join([
            str(res) for res in self.results
        ])
        return f'QueryReply({content})'
