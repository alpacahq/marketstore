"""
STATUS:OK for 1sec timeframe. NOK for 1Min timeframe but the failure cases are skipped.
BUG:issue with leakage with timeframe=1Min for some conditions (cf. tests symbols= [S_TEST_F1, S_TEST_F2])
NOTE: to run tests with expected failure, add the --runxfail option to pytest
"""
import pytest
import random
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
        # without nanoseconds
        ################################################################################
        (
                "S_TEST_1",
                "1Min",
                dict(Bid=np.arange(3), Ask=np.arange(3)),
                ["2016-01-01 10:01:00", "2016-01-01 10:01:30", "2016-01-01 10:01:59"],
                None,
                "2016-01-01 10:01:00",
                "2016-01-01 10:01:40",
        ),
        pytest.param(
            # BUG
            # for 1Min timeframe, the query will return all the ticks from start_dt to
            # the end of timeframe of end_dt
            #             input df
            #                            Bid  Ask
            # Epoch
            # 2016-01-01 10:01:30+00:00    0    0
            # 2016-01-01 10:01:50+00:00    1    1
            # 2016-01-01 10:02:10+00:00    2    2
            #
            # filtered input df
            # Empty DataFrame
            # Columns: [Bid, Ask]
            # Index: []
            #
            # output df, postprocessed
            #                                      Bid  Ask
            # Epoch
            # 2016-01-01 10:01:30+00:00              0    0
            # 2016-01-01 10:01:50.999999995+00:00    1    1
            #
            # output df, raw
            #                            Bid  Ask  Nanoseconds
            # Epoch
            # 2016-01-01 10:01:30+00:00    0    0            0
            # 2016-01-01 10:01:50+00:00    1    1    999999995
            # lengths do not match, inspect manually
            # query before 1st timeframe and same year in same bucket
            "S_TEST_F1",
            "1Min",
            dict(Bid=np.arange(3), Ask=np.arange(3)),
            ["2016-01-01 10:01:30", "2016-01-01 10:01:50", "2016-01-01 10:02:10"],
            None,
            "2016-01-01 10:01:00",
            "2016-01-01 10:01:20",
            marks=pytest.mark.xfail(reason="Known issue with 1Min timeframe.")
        ),
        pytest.param(
            # BUG same bug as above
            # for 1Min timeframe, the query will return all the ticks from start_dt to
            # the end of timeframe of end_dt
            #             input df
            #                            Bid  Ask
            # Epoch
            # 2016-01-01 10:00:00+00:00    0    0
            # 2016-01-01 10:00:59+00:00    1    1
            #
            # filtered input df
            # Empty DataFrame
            # Columns: [Bid, Ask]
            # Index: []
            #
            # output df, postprocessed
            #                                      Bid  Ask
            # Epoch
            # 2016-01-01 10:00:59.999999990+00:00    1    1
            #
            # output df, raw
            #                            Bid  Ask  Nanoseconds
            # Epoch
            # 2016-01-01 10:00:59+00:00    1    1    999999990
            # lengths do not match, inspect manually
            "S_TEST_F2",
            "1Min",
            dict(Bid=np.arange(2), Ask=np.arange(2)),
            ["2016-01-01 10:00:00", "2016-01-01 10:00:59"],
            None,
            "2016-01-01 10:00:10",
            "2016-01-01 10:00:40",
            marks=pytest.mark.xfail(reason="Known issue with 1Min timeframe.")
        ),
        # tests cases close to the timeframe border
        ################################################################################
        (
                "S_TEST_4",
                "1Min",
                dict(Bid=np.arange(3), Ask=np.arange(3)),
                ["2016-01-01 10:01:00", "2016-01-01 10:01:00", "2016-01-01 10:02:00"],
                None,
                "2016-01-01 10:01:00",
                "2016-01-01 10:01:00",
        ),
        (
                "S_TEST_5",
                "1Min",
                dict(Bid=np.arange(3), Ask=np.arange(3)),
                ["2016-01-01 10:01:00", "2016-01-01 10:01:00", "2016-01-01 10:02:00"],
                None,
                "2016-01-01 10:01:00",
                "2016-01-01 10:02:00",
        ),
        (
                "S_TEST_6",
                "1Min",
                dict(Bid=np.arange(3), Ask=np.arange(3)),
                ["2016-01-01 10:01:00", "2016-01-01 10:01:00", "2016-01-01 10:02:01"],
                None,
                "2016-01-01 10:01:00",
                "2016-01-01 10:02:00",
        ),
        (
                "S_TEST_7",
                "1Min",
                dict(Bid=np.arange(3), Ask=np.arange(3)),
                ["2016-01-01 10:01:00", "2016-01-01 10:02:01", "2016-01-01 10:02:01"],
                None,
                "2016-01-01 10:01:00",
                "2016-01-01 10:02:00",
        ),
        # with nanoseconds
        ################################################################################
        (
                "S_TEST_11",
                "1Min",
                dict(Bid=np.arange(3), Ask=np.arange(3)),
                ["2016-01-01 10:01:00", "2016-01-01 10:01:30", "2016-01-01 10:01:59"],
                [0, 0, 0],
                "2016-01-01 10:01:00",
                "2016-01-01 10:01:40",
        ),
        pytest.param(
            "S_TEST_13",
            "1Min",
            dict(Bid=np.arange(4), Ask=np.arange(4)),
            [
                "2016-01-01 10:01:00",
                "2016-01-01 10:01:59",
                "2016-01-01 10:02:00",
                "2016-01-01 10:02:01",
            ],
            [0, 0, 0, 0],
            "2016-01-01 10:01:00",
            "2016-01-01 10:02:00",
            marks=pytest.mark.xfail(reason="Known issue with 1Min timeframe.")
        ),
        # tests cases close to the timeframe border
        ################################################################################
        (
                "S_TEST_14",
                "1Min",
                dict(Bid=np.arange(3), Ask=np.arange(3)),
                ["2016-01-01 10:01:00", "2016-01-01 10:01:00", "2016-01-01 10:02:00"],
                [0, 0, 0],
                "2016-01-01 10:01:00",
                "2016-01-01 10:01:00",
        ),
        (
                "S_TEST_15",
                "1Min",
                dict(Bid=np.arange(3), Ask=np.arange(3)),
                ["2016-01-01 10:01:00", "2016-01-01 10:01:00", "2016-01-01 10:02:00"],
                [0, 0, 0],
                "2016-01-01 10:01:00",
                "2016-01-01 10:02:00",
        ),
        (
                "S_TEST_16",
                "1Min",
                dict(Bid=np.arange(3), Ask=np.arange(3)),
                ["2016-01-01 10:01:00", "2016-01-01 10:01:00", "2016-01-01 10:02:01"],
                [0, 0, 0],
                "2016-01-01 10:01:00",
                "2016-01-01 10:02:00",
        ),
        (
                "S_TEST_17",
                "1Min",
                dict(Bid=np.arange(3), Ask=np.arange(3)),
                ["2016-01-01 10:01:00", "2016-01-01 10:02:01", "2016-01-01 10:02:01"],
                [0, 0, 0],
                "2016-01-01 10:01:00",
                "2016-01-01 10:02:00",
        ),
    ],
)
def test_overflow_query_with_simple_data_1Min(
        symbol, timeframe, data, index, nanoseconds, start, end
):
    client.destroy(tbk=f"{symbol}/{timeframe}/TICK")

    start = pd.Timestamp(start, tz="utc")
    end = pd.Timestamp(end, tz="utc")

    in_df = utils.build_dataframe(
        data,
        pd.to_datetime(index, format="%Y-%m-%d %H:%M:%S").tz_localize("utc"),
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
        # without nanoseconds
        ################################################################################
        (
                "S_TEST_20",
                "1Sec",
                dict(Bid=np.arange(3), Ask=np.arange(3)),
                ["2016-01-01 10:00:01", "2016-01-01 10:00:01", "2016-01-01 10:00:02"],
                None,
                "2016-01-01 10:00:01",
                "2016-01-01 10:00:01",
        ),
        (
                "S_TEST_21",
                "1Sec",
                dict(Bid=np.arange(3), Ask=np.arange(3)),
                ["2016-01-01 10:00:01", "2016-01-01 10:00:01", "2016-01-01 10:00:02"],
                None,
                "2016-01-01 10:00:01",
                "2016-01-01 10:00:02",
        ),
        (
                "S_TEST_22",
                "1Sec",
                dict(Bid=np.arange(3), Ask=np.arange(3)),
                ["2016-01-01 10:00:01", "2016-01-01 10:00:01", "2016-01-01 10:00:02"],
                None,
                "2016-01-01 10:00:01",
                "2016-01-01 10:00:02",
        ),
        (
                "S_TEST_23",
                "1Sec",
                dict(Bid=np.arange(3), Ask=np.arange(3)),
                ["2016-01-01 10:00:01", "2016-01-01 10:00:02", "2016-01-01 10:00:02"],
                None,
                "2016-01-01 10:00:01",
                "2016-01-01 10:00:02",
        ),
        # with nanoseconds
        ################################################################################
        (
                "S_TEST_30",
                "1Sec",
                dict(Bid=np.arange(3), Ask=np.arange(3)),
                ["2016-01-01 10:00:01", "2016-01-01 10:00:01", "2016-01-01 10:00:02"],
                [0, 0, 0],
                "2016-01-01 10:00:01",
                "2016-01-01 10:00:01",
        ),
        (
                "S_TEST_31",
                "1Sec",
                dict(Bid=np.arange(3), Ask=np.arange(3)),
                ["2016-01-01 10:00:01", "2016-01-01 10:00:01", "2016-01-01 10:00:02"],
                [0, 0, 0],
                "2016-01-01 10:00:01",
                "2016-01-01 10:00:02",
        ),
        (
                # SUCCESS of S_TEST_32: this was previously a BUG with 1Sec timeframe
                # The old behaviour was not considering the nanosecond in the query parameter:
                # input df
                #                            Bid  Ask
                # Epoch
                # 2016-01-01 10:00:01+00:00    0    0
                # 2016-01-01 10:00:01+00:00    1    1
                #
                # output df, postprocessed
                #                                      Bid  Ask
                # Epoch
                # 2016-01-01 10:00:01+00:00              0    0
                # 2016-01-01 10:00:01+00:00              1    1
                # 2016-01-01 10:00:02.000000001+00:00    2    2
                #
                # output df, raw
                #                            Bid  Ask  Nanoseconds
                # Epoch
                # 2016-01-01 10:00:01+00:00    0    0            0
                # 2016-01-01 10:00:01+00:00    1    1            0
                # 2016-01-01 10:00:02+00:00    2    2            1
                "S_TEST_32",
                "1Sec",
                dict(Bid=np.arange(3), Ask=np.arange(3)),
                ["2016-01-01 10:00:01", "2016-01-01 10:00:01", "2016-01-01 10:00:02"],
                [0, 0, 1],
                "2016-01-01 10:00:01",
                "2016-01-01 10:00:02",
        ),
        (
                # SUCCESS of S_TEST_33: this was previously a BUG with 1Sec timeframe
                # The old behaviour was not considering the nanosecond in the query parameter:
                # input df
                #                            Bid  Ask
                # Epoch
                # 2016-01-01 10:00:01+00:00    0    0
                #
                # output df, postprocessed
                #                                      Bid  Ask
                # Epoch
                # 2016-01-01 10:00:01+00:00              0    0
                # 2016-01-01 10:00:02.000000001+00:00    1    1
                # 2016-01-01 10:00:02.000000001+00:00    2    2
                #
                # output df, raw
                #                            Bid  Ask  Nanoseconds
                # Epoch
                # 2016-01-01 10:00:01+00:00    0    0            0
                # 2016-01-01 10:00:02+00:00    1    1            1
                # 2016-01-01 10:00:02+00:00    2    2            1
                "S_TEST_33",
                "1Sec",
                dict(Bid=np.arange(3), Ask=np.arange(3)),
                ["2016-01-01 10:00:01", "2016-01-01 10:00:02", "2016-01-01 10:00:02"],
                [0, 1, 1],
                "2016-01-01 10:00:01",
                "2016-01-01 10:00:02",
        ),
    ],
)
def test_overflow_query_with_simple_data_1Sec(
        symbol, timeframe, data, index, nanoseconds, start, end
):
    """
    NOTE
        If nanoseconds==None, it will not be written. However, it might be implied by
        AttributeGroup=TICK. The default nanoseconds value is to be investigated on the
        marketsore side
    """
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
    "symbol, timeframe, size, start, window",
    [
        # potential BUG
        # if you set a very high density of ticks in a window, it increases the proba
        # to generate ticks with nanoseconds > 999 999 980 and to have the issue with
        # the flipping second
        # disabled to speed up tests
        ("RD_121", "1Sec", 10, "2016-01-01", "4H"),
        ("RD_122", "1Sec", 100, "2016-01-01", "4H"),
        ("RD_123", "1Sec", 1000, "2016-01-01", "4H"),
        ("RD_124", "1Sec", 10000, "2016-01-01", "4H"),
        ("RD_125", "1Sec", 100_000, "2016-01-01", "4H"),
        ("RD_126", "1Sec", 1_000_000, "2016-01-01", "4H"),
        ("RD_131", "1Sec", 10, "2016-01-01", "5Sec"),
        ("RD_132", "1Sec", 100, "2016-01-01", "5Sec"),
        ("RD_133", "1Sec", 1000, "2016-01-01", "5Sec"),
        ("RD_134", "1Sec", 10000, "2016-01-01", "5Sec"),
        ("RD_135", "1Sec", 100_000, "2016-01-01", "5Sec"),
        ("RD_136", "1Sec", 1_000_000, "2016-01-01", "5Sec"),
    ],
)
def test_overflow_query_with_random_data(
        symbol, timeframe, size, start, window
):
    client.destroy(tbk=f"{symbol}/{timeframe}/TICK")

    window = pd.Timedelta(window)
    start = pd.Timestamp(start, tz="utc")
    end = start + window

    np.random.seed(42)
    random.seed(42)

    # because we expect the some leakage within 1 second due to the nanoseconds field,
    # we add some margin to data around (not exactly super close to the central data)
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


def write_with_pymkts(df: pd.DataFrame, symbol: str, timeframe: str, extract_nanoseconds: bool = True):
    """
        Write with pymarketstore client: function to benchmark
    """
    records = utils.to_records(df, extract_nanoseconds=extract_nanoseconds)
    tbk = f"{symbol}/{timeframe}/TICK"
    return client.write(records, tbk, isvariablelength=True)


def build_test(in_df: pd.DataFrame, symbol: str, timeframe: str, start, end):
    param = pymkts.Params([symbol], timeframe, "TICK", start=start, end=end)

    out_df = client.query(param).first().df()

    # sort columns
    filtered_ind_df = in_df[(start <= in_df.index) & (in_df.index <= end)]
    converted_out_df = utils.process_query_result(out_df, inplace=False)

    print("\ninput df")
    print(in_df)
    print("\nfiltered input df")
    print(filtered_ind_df)
    print("\noutput df, postprocessed")
    print(converted_out_df)
    print("\noutput df, raw")
    print(out_df)

    assert not out_df.empty

    try:
        pd.testing.assert_frame_equal(filtered_ind_df, converted_out_df)
    except AssertionError as e:
        df1 = filtered_ind_df
        df2 = converted_out_df

        if len(df1) != len(df2):
            print("lengths do not match, inspect manually")
            raise

        bad_locations = df1.index != df2.index
        dilated_bad_locations = np.convolve(
            bad_locations.view(int), [1, 1, 1], mode="same"
        ).view(bool)
        print("Show dilated bad locations".center(40, "-"))
        print("\ninput df")
        # display(df1.loc[dilated_bad_locations, :])
        print("\noutput df, postprocessed")
        # display(df2.loc[dilated_bad_locations, :])
        print("\noutput df, raw")
        # display(out_df.loc[dilated_bad_locations, :])

        raise
