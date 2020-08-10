"""
STATUS:~OK -> Mismatch of output between jsonrpc and grpcs but acceptable. Using grpc produces a lot of error logs. Needs to be fixed.
MIGRATION_STATUS:OK
"""
import logging
import os

import pymarketstore as pymkts
from pymarketstore import ListSymbolsFormat

use_grpc = os.getenv("USE_GRPC", "false") == "true"
client = pymkts.Client(f"http://127.0.0.1:{os.getenv('MARKETSTORE_PORT',5993)}/rpc",
                       grpc=use_grpc)
logger = logging.getLogger(__name__)


def clean_db():
    """
    Destroy all symbols stored in pymarketstore
    """

    tbks = client.list_symbols(fmt=ListSymbolsFormat.TBK)

    if not tbks:
        logger.info("No symbols to clean in the marketstore.")
        return

    for tbk in set(tbks):
        client.destroy(tbk)


def test_type_list_symbols_with_empty_database():
    clean_db()
    symbols = client.list_symbols()

    # TODO: Fix grpc client to remove verbose error logs.
    # BUG: The stdout/stderr? of grpc has a lot of logs, I think it should be fixed upstream:
    #  Exception ignored in: <bound method _ChannelCallState.__del__ of <grpc._channel._ChannelCallState object at 0x7fd99c92ea20>>
    # Traceback (most recent call last):
    #   File "/usr/local/lib/python3.6/dist-packages/grpc/_channel.py", line 1126, in __del__
    #   File "src/python/grpcio/grpc/_cython/_cygrpc/channel.pyx.pxi", line 515, in grpc._cython.cygrpc.Channel.close
    #   File "src/python/grpcio/grpc/_cython/_cygrpc/channel.pyx.pxi", line 399, in grpc._cython.cygrpc._close
    #   File "src/python/grpcio/grpc/_cython/_cygrpc/channel.pyx.pxi", line 429, in grpc._cython.cygrpc._close
    #   File "/usr/lib/python3.6/threading.py", line 364, in notify_all
    #   File "/usr/lib/python3.6/threading.py", line 347, in notify
    # TypeError: 'NoneType' object is not callable
    # Exception ignored in: <bound method _ChannelCallState.__del__ of <grpc._channel._ChannelCallState object at 0x7fd99c4a8390>>
    # Traceback (most recent call last):
    #   File "/usr/local/lib/python3.6/dist-packages/grpc/_channel.py", line 1126, in __del__
    # AttributeError: 'NoneType' object has no attribute 'cancelled'

    # (akkie) ^ this issue will be solved by the following PR: https://github.com/grpc/grpc/issues/23290
    assert symbols == []
