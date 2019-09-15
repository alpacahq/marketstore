import pymarketstore as pymkts
import os

host = os.environ.get('MARKETSTORE_HOST', 'localhost:5993')
client = pymkts.Client(endpoint='http://{}/rpc'.format(host))


def test_not_empty_database():
    symbols = client.list_symbols()
    assert symbols is not None
