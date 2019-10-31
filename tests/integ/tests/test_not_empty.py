import pymarketstore as pymkts

client = pymkts.Client(endpoint='http://localhost:5993/rpc')


def test_not_empty_database():
    symbols = client.list_symbols()
    assert symbols is not None