"""
STATUS:OK
NOTE:returns empty dataframe in periods when there is no data. 
TODO: improve exception when the symbol does not exist
MIGRATION_STATUS:OK
"""
import pytest

import numpy as np
import pandas as pd
import os
import pymarketstore as pymkts
from . import utils

client = pymkts.Client(f"http://127.0.0.1:{os.getenv('MARKETSTORE_PORT',5993)}/rpc",
                       grpc=(os.getenv("USE_GRPC", "false") == "true"))



@pytest.mark.parametrize(
    "symbol, timeframe, data, index, nanoseconds, start, end",
    [
        ################################################################################
        # 1Min timeframe
        ################################################################################
        (
                # query before 1st timeframe and different year
                "VA_TEST_1",
                "1Min",
                dict(Bid=np.arange(2), Ask=np.arange(2)),
                ["2016-01-01 10:01:00", "2016-01-01 10:02:00"],
                None,
                "2015-01-01 09:01:00",
                "2015-01-01 09:04:00",
        ),
        (
                # query before 1st timeframe and same year in different bucket
                "VA_TEST_2",
                "1Min",
                dict(Bid=np.arange(2), Ask=np.arange(2)),
                ["2016-01-01 10:01:00", "2016-01-01 10:02:00"],
                None,
                "2016-01-01 09:01:00",
                "2016-01-01 09:04:00",
        ),
        (
                # no data surrounded and outside year
                "VA_TEST_4",
                "1Min",
                dict(Bid=np.arange(2), Ask=np.arange(2)),
                ["2016-01-01 10:01:00", "2018-01-01 10:06:00"],
                None,
                "2017-01-01 10:02:00",
                "2017-01-01 10:04:00",
        ),
        (
                # no data surrounded and within year and not timeframe with data
                "VA_TEST_5",
                "1Min",
                dict(Bid=np.arange(2), Ask=np.arange(2)),
                ["2016-01-01 10:00:00", "2016-02-01 10:00:00"],
                None,
                "2016-01-05 10:00:00",
                "2016-01-06 10:00:00",
        ),
        (
                # query after 1st timeframe and different year
                "VA_TEST_7",
                "1Min",
                dict(Bid=np.arange(2), Ask=np.arange(2)),
                ["2016-01-01 10:01:00", "2016-01-01 10:02:00"],
                None,
                "2017-01-01 09:01:00",
                "2017-01-01 09:04:00",
        ),
        (
                # query after 1st timeframe and same year in different bucket
                "VA_TEST_8",
                "1Min",
                dict(Bid=np.arange(2), Ask=np.arange(2)),
                ["2016-01-01 10:01:00", "2016-01-01 10:02:00"],
                None,
                "2016-01-01 11:01:00",
                "2016-01-01 11:04:00",
        ),
        (
                # query after 1st timeframe and same year in same bucket
                "VA_TEST_9",
                "1Min",
                dict(Bid=np.arange(2), Ask=np.arange(2)),
                ["2016-01-01 10:01:00", "2016-01-01 10:01:10"],
                None,
                "2016-01-01 10:01:30",
                "2016-01-01 10:01:59",
        ),
        ################################################################################
        # 1Sec timeframe
        ################################################################################
        (
                # query before 1st timeframe and different year
                "VB_TEST_1",
                "1Sec",
                dict(Bid=np.arange(2), Ask=np.arange(2)),
                ["2016-01-01 10:01:00", "2016-01-01 10:02:00"],
                None,
                "2015-01-01 09:01:00",
                "2015-01-01 09:04:00",
        ),
        (
                # query before 1st timeframe and same year in different bucket
                "VB_TEST_2",
                "1Sec",
                dict(Bid=np.arange(2), Ask=np.arange(2)),
                ["2016-01-01 10:01:00", "2016-01-01 10:02:00"],
                None,
                "2016-01-01 09:01:00",
                "2016-01-01 09:04:00",
        ),
        (
                # query before 1st timeframe and same year in same bucket
                "VB_TEST_3",
                "1Sec",
                dict(Bid=np.arange(3), Ask=np.arange(3)),
                ["2016-01-01 10:01:30", "2016-01-01 10:01:50", "2016-01-01 10:02:10"],
                None,
                "2016-01-01 10:01:00",
                "2016-01-01 10:01:20",
        ),
        (
                # no data surrounded and outside year
                "VB_TEST_4",
                "1Sec",
                dict(Bid=np.arange(2), Ask=np.arange(2)),
                ["2016-01-01 10:01:00", "2018-01-01 10:06:00"],
                None,
                "2017-01-01 10:02:00",
                "2017-01-01 10:04:00",
        ),
        (
                # no data surrounded and within year and not timeframe with data
                "VB_TEST_5",
                "1Sec",
                dict(Bid=np.arange(2), Ask=np.arange(2)),
                ["2016-01-01 10:00:00", "2016-02-01 10:00:00"],
                None,
                "2016-01-05 10:00:00",
                "2016-01-06 10:00:00",
        ),
        (
                # no data surrounded and within year and timeframe
                "VB_TEST_6",
                "1Sec",
                dict(Bid=np.arange(2), Ask=np.arange(2)),
                ["2016-01-01 10:00:00", "2016-01-01 10:00:59"],
                None,
                "2016-01-01 10:00:10",
                "2016-01-01 10:00:40",
        ),
        (
                # query after 1st timeframe and different year
                "VB_TEST_7",
                "1Sec",
                dict(Bid=np.arange(2), Ask=np.arange(2)),
                ["2016-01-01 10:01:00", "2016-01-01 10:02:00"],
                None,
                "2017-01-01 09:01:00",
                "2017-01-01 09:04:00",
        ),
        (
                # query after 1st timeframe and same year in different bucket
                "VB_TEST_8",
                "1Sec",
                dict(Bid=np.arange(2), Ask=np.arange(2)),
                ["2016-01-01 10:01:00", "2016-01-01 10:02:00"],
                None,
                "2016-01-01 11:01:00",
                "2016-01-01 11:04:00",
        ),
        (
                # query after 1st timeframe and same year in same bucket
                "VB_TEST_9",
                "1Sec",
                dict(Bid=np.arange(2), Ask=np.arange(2)),
                ["2016-01-01 10:01:00", "2016-01-01 10:01:10"],
                None,
                "2016-01-01 10:01:30",
                "2016-01-01 10:01:59",
        ),
    ],
)
def test_no_data_available(symbol, timeframe, data, index, nanoseconds, start, end):
    start = pd.Timestamp(start, tz="utc")
    end = pd.Timestamp(end, tz="utc")

    in_df = utils.build_dataframe(
        data,
        pd.to_datetime(index, format="%Y-%m-%d %H:%M:%S").tz_localize("utc"),
        columns=["Bid", "Ask"],
        nanoseconds=nanoseconds,
    )
    records = utils.to_records(in_df, extract_nanoseconds=nanoseconds is not None)
    tbk = f"{symbol}/{timeframe}/TICK"

    # ---- given ----
    ret = client.write(records, tbk, isvariablelength=True)
    print("Msg ret: {}".format(ret))

    param = pymkts.Params([symbol], timeframe, "TICK", start=start, end=end)

    # ---- when ----
    ret = client.query(param)
    out_df = ret.first().df()

    assert out_df.empty


@pytest.mark.parametrize(
    "symbol, timeframe, start, end",
    [("TEST_NOT_EXIST", "1Sec", "2016-01-01 10:01:30", "2016-01-01 10:01:59")],
)
def test_symbol_does_not_exist(symbol, timeframe, start, end):
    start = pd.Timestamp(start, tz="utc")
    end = pd.Timestamp(end, tz="utc")

    param = pymkts.Params([symbol], "1Sec", "TICK", start=start, end=end)
    with pytest.raises(Exception) as excinfo:
        # resp = {'error': {'code': -32000, 'data': None, 'message': 'no files returned from query parse'}, 'id': '1', 'jsonrpc': '2.0'} # noqa
        # pymarketstore/jsonrpc.py:48: Exception
        client.query(param)
    assert "no files returned from query parse" in str(excinfo.value)
