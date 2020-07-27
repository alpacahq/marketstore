"""
STATUS:OK
TODO:clarify purpose of this test
NOTE:using isvariablelength=True will always create a Nanoseconds field in the marketstore.  Any field names Nanoseconds must be int32
MIGRATION_STATUS:OK
"""
import pytest

import grpc
import numpy as np
import pandas as pd
from datetime import datetime, timezone
import requests
import os
import pymarketstore as pymkts

use_grpc = os.getenv("USE_GRPC", "true") == "true"
client = pymkts.Client(f"http://127.0.0.1:{os.getenv('MARKETSTORE_PORT',5995)}/rpc",
                       grpc=use_grpc)


@pytest.mark.parametrize(
    "symbol, timeframe, nanosecond, attribute_group, is_variable_length, exp_columns",
    [
        ("TEST_1", "1Sec", None, "TICK", True, ["Ask", "Nanoseconds"]),
        ("TEST_2", "1Sec", None, "TICK", False, ["Ask"]),
        ("TEST_3", "1Sec", None, "Tick", True, ["Ask", "Nanoseconds"]),
        ("TEST_4", "1Sec", None, "Tick", False, ["Ask"]),
        ("TEST_11", "1Sec", 0, "TICK", True, ["Ask", "Nanoseconds"]),
        ("TEST_12", "1Sec", 0, "TICK", False, ["Ask", "Nanoseconds"]),
        ("TEST_13", "1Sec", 0, "Tick", True, ["Ask", "Nanoseconds"]),
        ("TEST_14", "1Sec", 0, "Tick", False, ["Ask", "Nanoseconds"]),
    ],
)
def test_ticks_fields(
        symbol, timeframe, nanosecond, attribute_group, is_variable_length, exp_columns
):
    if nanosecond is None:
        data = np.array(
            [(pd.Timestamp("2017-01-01 00:00").value / 10 ** 9, 10.0)],
            dtype=[("Epoch", "i8"), ("Ask", "f4")],
        )
    else:
        data = np.array(
            [(pd.Timestamp("2017-01-01 00:00").value / 10 ** 9, 10.0, nanosecond)],
            dtype=[("Epoch", "i8"), ("Ask", "f4"), ("Nanoseconds", "i4")],
        )

    client.write(
        data,
        f"{symbol}/{timeframe}/{attribute_group}",
        isvariablelength=is_variable_length,
    )

    d2 = client.query(pymkts.Params(symbol, timeframe, attribute_group)).first().df()
    print("Length of result: ", d2.shape[0])
    assert d2.shape[0] == 1
    assert (
            datetime(2017, 1, 1, 0, 0, 0, tzinfo=timezone.utc).timestamp()
            == d2.index[0].timestamp()
    )

    print(d2)

    assert d2.Ask.dtype == np.float32

    if "Nanoseconds" in d2.columns:
        assert d2.Nanoseconds.dtype == np.int32

    assert d2.columns.tolist() == exp_columns


@pytest.mark.parametrize(
    "symbol, timeframe, epoch_dtype, nanosecond_dtype, nanosecond, "
    "attribute_group, is_variable_length, status",
    [
        # # success
        # ("TEST_1", "1Sec", "i8", None, None, "TICK", True, "SUCCESS"),
        # ("TEST_2", "1Sec", "i8", None, None, "TICK", False, "SUCCESS"),
        # ("TEST_1", "1Sec", "i8", "i4", 0, "TICK", True, "SUCCESS"),
        # ("TEST_2", "1Sec", "i8", "i4", 0, "TICK", False, "SUCCESS"),
        # # failures: wrong epoch dtype
        # ("TEST_1", "1Sec", "i4", None, None, "TICK", True, "FAILURE"),
        # ("TEST_2", "1Sec", "i4", None, None, "TICK", False, "FAILURE"),
        # ("TEST_1", "1Sec", "i4", "i4", 0, "TICK", True, "FAILURE"),
        # ("TEST_2", "1Sec", "i4", "i4", 0, "TICK", False, "FAILURE"),
        # # failures: wrong nanoseconds dtype
        ("TEST_1", "1Sec", "i8", "i8", 0, "TICK", True, "FAILURE"),
        # ("TEST_2", "1Sec", "i8", "i8", 0, "TICK", False, "FAILURE"),
    ],
)
def test_ticks_types(
        symbol,
        timeframe,
        epoch_dtype,
        nanosecond_dtype,
        nanosecond,
        attribute_group,
        is_variable_length,
        status,
):
    client.destroy(tbk=f"{symbol}/{timeframe}/{attribute_group}")

    if nanosecond is None:
        data = np.array(
            [(pd.Timestamp("2017-01-01 00:00").value / 10 ** 9, 10.0)],
            dtype=[("Epoch", epoch_dtype), ("Ask", "f4")],
        )
    else:
        data = np.array(
            [(pd.Timestamp("2017-01-01 00:00").value / 10 ** 9, 10.0, nanosecond)],
            dtype=[
                ("Epoch", epoch_dtype),
                ("Ask", "f4"),
                ("Nanoseconds", nanosecond_dtype),
            ],
        )

    if status == "FAILURE":
        with pytest.raises((requests.exceptions.ConnectionError, grpc.RpcError)) as exc_info:
            # With jsonrpc:
            # uninformative exception is raised:
            # * exception raised on python side:
            # requests.exceptions.ConnectionError: ('Connection aborted.', RemoteDisconnected('Remote end closed connection without response',))  # noqa
            # With grpc:
            # raise _InactiveRpcError(state)
            #  grpc._channel._InactiveRpcError: <_InactiveRpcError of RPC that terminated with:
            #   status = StatusCode.UNAVAILABLE
            #   details = "failed to connect to all addresses"
            #   debug_error_string = "{"created":"@1594022914.472163378","description":"Failed to pick subchannel","file":"src/core/ext/filters/client_channel/client_channel.cc","file_line":3948,"referenced_errors":[{"created":"@1594022914.143041824","description":"failed to connect to all addresses","file":"src/co
            # re/ext/filters/client_channel/lb_policy/pick_first/pick_first.cc","file_line":394,"grpc_status":14}]}"
            ret = client.write(
                data,
                f"{symbol}/{timeframe}/{attribute_group}",
                isvariablelength=is_variable_length,
            )

        if use_grpc:
            assert exc_info.value.code() == grpc.StatusCode.UNAVAILABLE

    else:
        ret = client.write(
            data,
            f"{symbol}/{timeframe}/{attribute_group}",
            isvariablelength=is_variable_length,
        )
        print(ret)
