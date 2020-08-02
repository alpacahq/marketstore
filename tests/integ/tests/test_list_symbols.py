"""
STATUS:~OK -> Mismatch of output between jsonrpc and grpcs but acceptable. Using grpc produces a lot of error logs. Needs to be fixed.
MIGRATION_STATUS:OK
"""
import os

import pymarketstore as pymkts
import logging

use_grpc = os.getenv("USE_GRPC", "false") == "true"
client = pymkts.Client(f"http://127.0.0.1:{os.getenv('MARKETSTORE_PORT',5993)}/rpc",
                       grpc=use_grpc)
logger = logging.getLogger(__name__)


def clean_db():
    """
    Destroy symbols for timeframes=[1Sec, 1Min] and AttributeGroup=[TICK].
    """
    symbols = client.list_symbols()

    if not symbols:
        logger.info("No symbols to clean in the marketstore.")
        return

    destroyed_symbols = []
    error_symbols = []
    for symbol in set(symbols):
        for tf in ['1Sec', '1Min']:
            try:

                tbk = "{symbol}/{tf}/TICK".format(symbol=symbol, tf=tf)
                ret = client.destroy(tbk)
                responses = ret["responses"]

                if responses is not None:
                    if len(responses) != 1 or responses[0].get('error', None):
                        raise Exception("Some error:{}".format(responses))

            except Exception as e:
                logger.exception("Caught an error when tearing down marketstore")
                print(e)
                error_symbols.append((tf, symbol))
                continue

            destroyed_symbols.append((tf, symbol))

    logger.info("Failed to destroy:{}".format(error_symbols))
    logger.info("Destroyed:{}".format(destroyed_symbols))


def test_type_list_symbols_with_empty_database():
    clean_db()
    symbols = client.list_symbols()

    if use_grpc:
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
    else:
        assert symbols is None
