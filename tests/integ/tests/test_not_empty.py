import pymarketstore as pymkts
import os

client = pymkts.Client(f"http://127.0.0.1:{os.getenv('MARKETSTORE_PORT',5993)}/rpc",
                       grpc=(os.getenv("USE_GRPC", "false") == "true"))


def test_not_empty_database():
    symbols = client.list_symbols()
    assert symbols is not None
