"""
STATUS:OK, bug solved by https://github.com/alpacahq/marketstore/pull/249
MIGRATION_STATUS:OK
"""
import pytest
import random

import numpy as np
import pandas as pd
from datetime import datetime, timezone
import os

import pymarketstore as pymkts

from . import utils

client = pymkts.Client(f"http://127.0.0.1:{os.getenv('MARKETSTORE_PORT',5993)}/rpc",
                       grpc=(os.getenv("USE_GRPC", "false") == "true"))


@pytest.mark.parametrize(
    "symbol, timeframe, size, start, window",
    [
        # on 2 consecutive years
        ("RD_TEST_01", "1Min", 10000, "2015-12-25", "10D"),
        ("RD_TEST_02", "1Sec", 10000, "2015-12-25", "10D"),
        # on 3 consecutive years
        ("RD_TEST_03", "1Min", 10000, "2015-12-25", "900D"),
        ("RD_TEST_04", "1Sec", 10000, "2015-12-25", "900D"),
    ],
)
def test_overflow_query_with_random_data(
        symbol, timeframe, size, start, window
):
    client.destroy(tbk = f"{symbol}/{timeframe}/TICK")

    window = pd.Timedelta(window)
    start = pd.Timestamp(start, tz="utc")
    end = start + window

    np.random.seed(42)
    random.seed(42)

    pre_in_df = utils.generate_dataframe(
        size, start - window, start - pd.Timedelta("1s"), random_data=False
    )
    in_df = utils.generate_dataframe(size, start, end, random_data=False)
    post_in_df = utils.generate_dataframe(
        size, end + pd.Timedelta("1s"), end + window, random_data=False
    )
    write_with_pymkts(pre_in_df, symbol, timeframe, extract_nanoseconds=True)
    write_with_pymkts(in_df, symbol, timeframe, extract_nanoseconds=True)
    write_with_pymkts(post_in_df, symbol, timeframe, extract_nanoseconds=True)

    build_test(in_df, symbol, timeframe, start, end)


def write_with_pymkts(df, symbol, timeframe, extract_nanoseconds=True):
    """
        Write with pymarketstore client: function to benchmark
    """
    records = utils.to_records(df, extract_nanoseconds=extract_nanoseconds)
    tbk = f"{symbol}/{timeframe}/TICK"
    return client.write(records, tbk, isvariablelength=True)


def build_test(in_df, symbol, timeframe, start, end):
    param = pymkts.Params([symbol], timeframe, "TICK", start=start, end=end)

    out_df = client.query(param).first().df()

    df1 = in_df[(start <= in_df.index) & (in_df.index <= end)]
    df2 = utils.process_query_result(out_df, inplace=False)

    print("\ninput df")
    print(in_df)
    print("\nfiltered input df")
    print(df1)
    print("\noutput df, postprocessed")
    print(df2)
    print("\noutput df, raw")
    print(out_df)

    assert not out_df.empty

    if len(df1) != len(df2):
        print("lengths do not match, inspect manually")
        assert False

    # due to nanoseconds precision issue
    if timeframe == "1Min":
        df1 = df1.reset_index(drop=True)
        df2 = df2.reset_index(drop=True)

    try:
        pd.testing.assert_frame_equal(df1, df2)
    except AssertionError as e:

        bad_locations = df1.index != df2.index
        dilated_bad_locations = np.convolve(
            bad_locations.astype(int), [1, 1, 1], mode="same"
        ).astype(bool)
        print("Show dilated bad locations".center(40, "-"))
        print("\ninput df")
        print(df1.loc[dilated_bad_locations, :])
        print("\noutput df, postprocessed")
        print(df2.loc[dilated_bad_locations, :])
        print("\noutput df, raw")
        print(out_df.loc[dilated_bad_locations, :])

        raise


@pytest.mark.parametrize(
    "symbol, isvariablelength, timeframe",
    [
        ("TOVR_BUG_1", True, "1Min"),
        ("TOVR_BUG_2", False, "1Min"),
        ("TOVR_BUG_3", True, "1Sec"),
        ("TOVR_BUG_4", False, "1Sec"),
    ],
)
def test_query_edges_on_overlapping_years(symbol, isvariablelength, timeframe):
    # original bug fixed by https://github.com/alpacahq/marketstore/pull/249

    data = np.array(
        [
            (pd.Timestamp("2017-01-01 00:00").value / 10 ** 9, 10.0),
            (pd.Timestamp("2018-01-01 00:00").value / 10 ** 9, 11.0),
        ],
        dtype=[("Epoch", "i8"), ("Ask", "f4")],
    )

    cli = utils.get_pymkts_client()
    cli.write(data, f"{symbol}/{timeframe}/TICK", isvariablelength=isvariablelength)

    params = pymkts.Params(
        symbol,
        timeframe,
        "TICK",
        start=pd.Timestamp("2017-01-01 00:00"),
        end=pd.Timestamp("2018-01-02 00:00"),
    )
    d_all = cli.query(params).first().df()

    display(d_all)
    assert d_all.shape[0] == 2
    assert datetime(2017, 1, 1, 0, 0, 0, tzinfo=timezone.utc) == d_all.index[0]
    assert datetime(2018, 1, 1, 0, 0, 0, tzinfo=timezone.utc) == d_all.index[-1]
