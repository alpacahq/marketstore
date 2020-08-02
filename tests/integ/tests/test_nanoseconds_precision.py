"""
STATUS:NOK, 4 failed
BUG: issue with nanoseconds precision when timeframe=1Min
BUG: issue with second flipping nanoseconds with timeframe=1Sec when nanosecond is > 999 999 880
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


def process_query_result(df: pd.DataFrame, inplace: bool = True) -> pd.DataFrame:
    """Posprocess the result of a query with pymarketstore.

    If the dataframe contains a Nanoseconds column (as a query to TICK data would do),
    we add the nanoseconds back to the index properly.

    Args:
        df      : A DataFrame returned by a query with pymarkestore.
        inplace : If True, the operation will be done inplace.
            This function is used in the benchmark for query.
            In real settings, we won't copy the dataframe for performance reasons.

    Returns:
        The posprocessed DataFrame with `Nanoseconds` column dropped and added to the
        time index.
    """

    if not inplace:
        df = df.copy()

    if "Nanoseconds" in df.columns:
        df.index = pd.to_datetime(
            df.index.values.astype("datetime64[s]")
            + df["Nanoseconds"].values.astype("timedelta64[ns]"),
            utc=True,
        )
        df.index.name = "Epoch"
        df.drop("Nanoseconds", axis=1, inplace=True)

    # to be aligned with the generated dataframe
    df.sort_index(axis=1, inplace=True)

    return df


@pytest.mark.parametrize(
    "symbol, timeframe, data, index, nanoseconds, start, end",
    [
        pytest.param(
            # this is a knwown BUG with 1Min timeframe
            # even if no nanoseconds field is not explicitly written, it is
            # implied by the isvariablelength=True when writing
            "BUG_1MIN_1",
            "1Min",
            dict(Bid=np.arange(3), Ask=np.arange(3)),
            ["2016-01-01 10:01:00", "2016-01-01 10:01:30", "2016-01-01 10:01:59"],
            None,
            "2016-01-01 00:00:00",
            "2016-01-01 23:59:59",
            marks=pytest.mark.xfail(reason="Known issue with 1Min timeframe."),
        ),
        pytest.param(
            # this is a knwown BUG with 1Min timeframe
            # same as above but here we explicitly write the nanoseconds
            "BUG_1MIN_WITH_NANOSECONDS",
            "1Min",
            dict(Bid=np.arange(3), Ask=np.arange(3)),
            ["2016-01-01 10:01:00", "2016-01-01 10:01:30", "2016-01-01 10:01:59"],
            [0, 0, 999_999_999],
            "2016-01-01 00:00:00",
            "2016-01-01 23:59:59",
            marks=pytest.mark.xfail(reason="Known issue with 1Min timeframe."),
        ),
    ],
)
def test_nanoseconds_precision_with_1Min_timeframe(
        symbol, timeframe, data, index, nanoseconds, start, end
):
    client.destroy(tbk=f"{symbol}/{timeframe}/TICK")

    start = pd.Timestamp(start, tz="utc")
    end = pd.Timestamp(end, tz="utc")

    in_df = utils.build_dataframe(
        data,
        pd.to_datetime(index, format="%Y-%m-%d %H:%M:%S").tz_localize("utc"),
        ["Bid", "Ask"],
        nanoseconds=nanoseconds,
    )

    with_nanoseconds = nanoseconds is not None
    ret = write_with_pymkts(
        in_df, symbol, timeframe, extract_nanoseconds=with_nanoseconds
    )

    print(ret)

    build_test(in_df, symbol, timeframe, start, end)


@pytest.mark.parametrize(
    "symbol, timeframe, data, index, nanoseconds, start, end",
    [
        pytest.param(
            # BUG
            # * Minimal test to reproduce second flipping at extreme edge of second
            # * It shows that the second flipping occurs when nanoseconds
            # value is > 999999880
            "TEST_SHOWING_SECOND_FLIP",
            "1Sec",
            dict(Bid=np.arange(300), Ask=np.arange(300)),
            ["2016-01-01 00:00:03"] * 300,
            np.arange(999_999_699, 999_999_999),
            "2016-01-01 00:00:00",
            "2016-01-01 23:59:59",
            marks=pytest.mark.xfail(reason="Known issue with 1Min timeframe."),
        ),
        pytest.param(
            # BUG (same as above)
            # * here showing that when restricting the query parameters, the data is
            #  filtered upstream according to the value of the second (so the shape
            # will be different)
            "TEST_SHOWING_WRONG_IDX_IS_FILTERED_UPSTREAM",
            "1Sec",
            dict(Bid=np.arange(300), Ask=np.arange(300)),
            ["2016-01-01 00:00:03"] * 300,
            np.arange(999_999_699, 999_999_999),
            "2016-01-01 00:00:03",
            "2016-01-01 00:00:03",
            marks=pytest.mark.xfail(reason="Known issue with 1Min timeframe."),
        ),
    ],
)
def test_second_flip_with_1Sec_timeframe(symbol, timeframe, data, index, nanoseconds, start, end):
    start = pd.Timestamp(start, tz="utc")
    end = pd.Timestamp(end, tz="utc")

    in_df = utils.build_dataframe(
        data,
        pd.to_datetime(index, format="%Y-%m-%d %H:%M:%S").tz_localize("utc"),
        ["Bid", "Ask"],
        nanoseconds=nanoseconds,
    )

    with_nanoseconds = nanoseconds is not None
    ret = write_with_pymkts(
        in_df, symbol, timeframe, extract_nanoseconds=with_nanoseconds
    )

    print(ret)

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
    processed_out_df = process_query_result(out_df, inplace=False)

    assert not out_df.empty

    try:
        pd.testing.assert_frame_equal(in_df, processed_out_df)
    except AssertionError:
        df1 = in_df
        df2 = processed_out_df

        if len(df1) != len(df2):
            print("lengths do not match, inspect manually")
            raise

        bad_locations = df1.index != df2.index
        dilated_bad_locations = np.convolve(
            bad_locations.astype(int), [1, 1, 1], mode="same"
        ).astype(bool)
        # print("Show dilated bad locations".center(40, "-"))
        # print("\ninput df")
        # display(df1.loc[dilated_bad_locations, :])
        # print("\noutput df, postprocessed")
        # display(df2.loc[dilated_bad_locations, :])
        # print("\noutput df, raw")
        # display(out_df.loc[dilated_bad_locations, :])

        raise
