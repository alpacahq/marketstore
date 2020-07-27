"""
STATUS:OK, data integrity is respected (only tested with pymarketstore)
always returns stable sorted ticks! Wow!
support consecutive ticks writing without overridding ticks
default behaviour is insert.
Since the queried data is always sorted by index + nanoseconds
TODO: investigate if this is done during writing or querying)
-> it would be nice to be able to determine the order of ticks
based on another field such as UniversalID
MIGRATION_STATUS:OK
"""
import pytest

import numpy as np
import pandas as pd
import random
import os

import pymarketstore as pymkts

from . import utils

client = pymkts.Client(f"http://127.0.0.1:{os.getenv('MARKETSTORE_PORT',5993)}/rpc",
                       grpc=(os.getenv("USE_GRPC", "false") == "true"))

def write(df, symbol, extract_nanoseconds=True):
    """
        Util to write ticks with pymarketstore client
    """
    records = utils.to_records(df, extract_nanoseconds=extract_nanoseconds)
    tbk = f"{symbol}/1Sec/TICK"
    return client.write(records, tbk, isvariablelength=True)


@pytest.mark.parametrize(
    "symbol, data, index, nanoseconds",
    [
        (
            "MULTIPLE_TICK_IN_MULTIPLE_TIMEFRAMES",
            dict(Bid=np.arange(3), Ask=np.arange(3)),
            ["2016-01-01 10:00:01", "2016-01-01 10:00:02", "2016-01-01 10:00:03"],
            None,
        ),
        (
            "MULTIPLE_TICK_IN_MULTIPLE_TIMEFRAMES_WITH_NANOSECONDS",
            dict(Bid=np.arange(3), Ask=np.arange(3)),
            ["2016-01-01 10:00:01", "2016-01-01 10:00:02", "2016-01-01 10:00:03"],
            [0, 1, 2],
        ),
        (
            "MULTIPLE_TICK_IN_TIMEFRAME_WITH_NANOSECONDS",
            dict(Bid=np.arange(2), Ask=np.arange(2)),
            ["2016-01-01 10:00:00", "2016-01-01 10:00:00"],
            [0, 1],
        ),
        (
            "DUPLICATED_INDEX_WITH_DIFFERENT_VALUES_1",
            dict(Bid=np.arange(2), Ask=np.arange(2)),
            ["2016-01-01 10:00:00", "2016-01-01 10:00:00"],
            None,
        ),
        (
            "DUPLICATED_INDEX_WITH_SAME_VALUES_1",
            dict(Bid=np.ones(2), Ask=np.ones(2)),
            ["2016-01-01 10:00:00", "2016-01-01 10:00:00"],
            None,
        ),
        (
            "DUPLICATED_INDEX_WITH_DIFFERENT_VALUES_WITH_NANOSECONDS",
            dict(Bid=np.arange(2), Ask=np.arange(2)),
            ["2016-01-01 10:00:00", "2016-01-01 10:00:00"],
            [1, 1],
        ),
        (
            "DUPLICATED_INDEX_WITH_SAME_VALUES_WITH_NANOSECONDS",
            dict(Bid=np.ones(2), Ask=np.ones(2)),
            ["2016-01-01 10:00:00", "2016-01-01 10:00:00"],
            [1, 1],
        ),
        (
            "NANOSECONDS_PRECISION_IN_SINGLE_TIMEFRAME",
            dict(Bid=np.arange(3), Ask=np.arange(3)),
            ["2016-01-01 10:00:00", "2016-01-01 10:00:00", "2016-01-01 10:00:00"],
            [0, 500_000_000, 999_999_880],
        ),
        (
            "NANOSECONDS_PRECISION_IN_MULTIPLE_TIMEFRAME",
            dict(Bid=np.arange(3), Ask=np.arange(3)),
            ["2016-01-01 10:00:00", "2016-01-01 10:00:01", "2016-01-01 10:00:01"],
            [0, 500_000_000, 999_999_880],
        ),
    ],
)
def test_ticks_simple_cases(symbol, data, index, nanoseconds):
    client.destroy(tbk = f"{symbol}/1Sec/TICK")

    in_df = utils.build_dataframe(
        data,
        pd.to_datetime(index, format="%Y-%m-%d %H:%M:%S").tz_localize("utc"),
        nanoseconds=nanoseconds,
    )

    ret = write(in_df, symbol, extract_nanoseconds=nanoseconds is not None)

    print("Msg ret: {}".format(ret))

    param = pymkts.Params([symbol], "1Sec", "TICK")

    out_df = client.query(param).first().df()
    processed_out_df = utils.process_query_result(out_df, inplace=False)

    print("\ninput df")
    print(in_df)
    print("\noutput df, postprocessed")
    print(processed_out_df)
    print("\noutput df, raw")
    print(out_df)

    pd.testing.assert_frame_equal(in_df, processed_out_df, check_less_precise=True)


@pytest.mark.parametrize(
    "symbol, tuples, expected_index",
    [
        (
            "DUPLICATED_INDEX_AND_VALUES",
            [
                (
                    dict(Bid=np.arange(3), Ask=np.arange(3)),
                    [
                        "2016-01-01 10:00:01",
                        "2016-01-01 10:00:02",
                        "2016-01-01 10:00:03",
                    ],
                    None,
                ),
                (
                    dict(Bid=np.arange(3), Ask=np.arange(3)),
                    [
                        "2016-01-01 10:00:01",
                        "2016-01-01 10:00:02",
                        "2016-01-01 10:00:03",
                    ],
                    None,
                ),
            ],
            [
                "2016-01-01 10:00:01.000000000",
                "2016-01-01 10:00:01.000000000",
                "2016-01-01 10:00:02.000000000",
                "2016-01-01 10:00:02.000000000",
                "2016-01-01 10:00:03.000000000",
                "2016-01-01 10:00:03.000000000",
            ],
        ),
        (
            "DUPLICATED_INDEX_AND_VALUES_WITH_DUPLICATED_NANOSECONDS",
            [
                (
                    dict(Bid=np.arange(3), Ask=np.arange(3)),
                    [
                        "2016-01-01 10:00:01",
                        "2016-01-01 10:00:02",
                        "2016-01-01 10:00:03",
                    ],
                    [0, 1, 2],
                ),
                (
                    dict(Bid=np.arange(3), Ask=np.arange(3)),
                    [
                        "2016-01-01 10:00:01",
                        "2016-01-01 10:00:02",
                        "2016-01-01 10:00:03",
                    ],
                    [0, 1, 2],
                ),
            ],
            [
                "2016-01-01 10:00:01.000000000",
                "2016-01-01 10:00:01.000000000",
                "2016-01-01 10:00:02.000000001",
                "2016-01-01 10:00:02.000000001",
                "2016-01-01 10:00:03.000000002",
                "2016-01-01 10:00:03.000000002",
            ],
        ),
        (
            "DUPLICATED_INDEX_AND_VALUES_WITH_UNDUPLICATED_NANOSECONDS",
            [
                (
                    dict(Bid=np.arange(3), Ask=np.arange(3)),
                    [
                        "2016-01-01 10:00:01",
                        "2016-01-01 10:00:02",
                        "2016-01-01 10:00:03",
                    ],
                    [0, 1, 2],
                ),
                (
                    dict(Bid=np.arange(3), Ask=np.arange(3)),
                    [
                        "2016-01-01 10:00:01",
                        "2016-01-01 10:00:02",
                        "2016-01-01 10:00:03",
                    ],
                    [4, 5, 6],
                ),
            ],
            [
                "2016-01-01 10:00:01.000000000",
                "2016-01-01 10:00:01.000000004",
                "2016-01-01 10:00:02.000000001",
                "2016-01-01 10:00:02.000000005",
                "2016-01-01 10:00:03.000000002",
                "2016-01-01 10:00:03.000000006",
            ],
        ),
        (
            "DUPLICATED_INDEX_AND_VALUES_WITH_UNSORTED_NANOSECONDS",
            [
                (
                    dict(Bid=np.arange(3), Ask=np.arange(3)),
                    [
                        "2016-01-01 10:00:01",
                        "2016-01-01 10:00:02",
                        "2016-01-01 10:00:03",
                    ],
                    [6, 5, 4],
                ),
                (
                    dict(Bid=np.arange(3), Ask=np.arange(3)),
                    [
                        "2016-01-01 10:00:01",
                        "2016-01-01 10:00:02",
                        "2016-01-01 10:00:03",
                    ],
                    [3, 2, 1],
                ),
            ],
            [
                "2016-01-01 10:00:01.000000003",
                "2016-01-01 10:00:01.000000006",
                "2016-01-01 10:00:02.000000002",
                "2016-01-01 10:00:02.000000005",
                "2016-01-01 10:00:03.000000001",
                "2016-01-01 10:00:03.000000004",
            ],
        ),
        (
            "DUPLICATED_INDEX_AND_VALUES_WITH_UNSORTED_GROUPS_OF_NANOSECONDS",
            [
                (
                    dict(Bid=np.arange(3), Ask=np.arange(3)),
                    [
                        "2016-01-01 10:00:01",
                        "2016-01-01 10:00:02",
                        "2016-01-01 10:00:03",
                    ],
                    [4, 5, 6],
                ),
                (
                    dict(Bid=np.arange(3), Ask=np.arange(3)),
                    [
                        "2016-01-01 10:00:01",
                        "2016-01-01 10:00:02",
                        "2016-01-01 10:00:03",
                    ],
                    [1, 2, 3],
                ),
            ],
            [
                "2016-01-01 10:00:01.000000001",
                "2016-01-01 10:00:01.000000004",
                "2016-01-01 10:00:02.000000002",
                "2016-01-01 10:00:02.000000005",
                "2016-01-01 10:00:03.000000003",
                "2016-01-01 10:00:03.000000006",
            ],
        ),
        (
            "DUPLICATED_INDEX_AND_VALUES_WITH_DUPLICATED_NANOSECONDS_WITHIN_TIMEFRAME",
            [
                (
                    dict(Bid=np.arange(3), Ask=np.arange(3)),
                    [
                        "2016-01-01 10:00:00",
                        "2016-01-01 10:00:00",
                        "2016-01-01 10:00:00",
                    ],
                    [0, 1, 2],
                ),
                (
                    dict(Bid=np.arange(3), Ask=np.arange(3)),
                    [
                        "2016-01-01 10:00:00",
                        "2016-01-01 10:00:00",
                        "2016-01-01 10:00:00",
                    ],
                    [4, 5, 6],
                ),
            ],
            [
                "2016-01-01 10:00:00.000000000",
                "2016-01-01 10:00:00.000000001",
                "2016-01-01 10:00:00.000000002",
                "2016-01-01 10:00:00.000000004",
                "2016-01-01 10:00:00.000000005",
                "2016-01-01 10:00:00.000000006",
            ],
        ),
        (
            "WITHIN_MULTIPLE_TIMEFRAME",
            [
                (
                    dict(Bid=np.arange(3), Ask=np.arange(3)),
                    [
                        "2016-01-01 10:01:00",
                        "2016-01-01 10:02:00",
                        "2016-01-01 10:03:00",
                    ],
                    [0, 1, 2],
                ),
                (
                    dict(Bid=np.arange(3), Ask=np.arange(3)),
                    [
                        "2016-01-01 10:01:00",
                        "2016-01-01 10:02:00",
                        "2016-01-01 10:03:00",
                    ],
                    [4, 5, 6],
                ),
            ],
            [
                "2016-01-01 10:01:00.000000000",
                "2016-01-01 10:01:00.000000004",
                "2016-01-01 10:02:00.000000001",
                "2016-01-01 10:02:00.000000005",
                "2016-01-01 10:03:00.000000002",
                "2016-01-01 10:03:00.000000006",
            ],
        ),
    ],
)
def test_ticks_multiple_writes(symbol, tuples, expected_index):
    client.destroy(tbk = f"{symbol}/1Sec/TICK")

    total_input_df_list = []
    for els in tuples:
        data, index, nanoseconds = els

        in_df = utils.build_dataframe(
            data,
            pd.to_datetime(index, format="%Y-%m-%d %H:%M:%S").tz_localize("utc"),
            nanoseconds=nanoseconds,
        )
        total_input_df_list.append(in_df)

        ret = write(in_df, symbol, extract_nanoseconds=nanoseconds is not None)

        print("Msg ret: {}".format(ret))

    total_input_df = pd.concat(total_input_df_list, axis=0)
    param = pymkts.Params([symbol], "1Sec", "TICK")

    out_df = client.query(param).first().df()
    processed_out_df = utils.process_query_result(out_df, inplace=False)

    print("\ntotal ninput df")
    print(total_input_df.sort_index(kind="merge"))
    print("\noutput df, postprocessed")
    print(processed_out_df)
    print("\noutput df, raw")
    print(out_df)

    assert (pd.to_datetime(expected_index, utc=True) == processed_out_df.index).all()
    pd.testing.assert_frame_equal(
        total_input_df.sort_index(kind="merge"),
        processed_out_df,
        check_less_precise=True,
    )


@pytest.mark.parametrize(
    "symbol, data, index, nanoseconds",
    [
        (
            "MULTIPLE_TICK_IN_SINGLE_TIMEFRAME_WIH_UNSORTED_NANOSECONDS",
            dict(Bid=np.arange(3), Ask=np.arange(3)),
            ["2016-01-01 10:00:00", "2016-01-01 10:00:00", "2016-01-01 10:00:00"],
            [3, 2, 1],
        ),
        (
            "UNSORTED_WITHIN_TIMEFRAME",
            dict(Bid=np.arange(3), Ask=np.arange(3)),
            ["2016-01-01 10:00:01", "2016-01-01 10:00:02", "2016-01-01 10:00:03"],
            [3, 2, 1],
        ),
        (
            "UNSORTED_WITHIN_MULTIPLE_TIMEFRAME",
            dict(Bid=np.arange(3), Ask=np.arange(3)),
            ["2016-01-01 10:00:03", "2016-01-01 10:00:02", "2016-01-01 10:00:01"],
            None,
        ),
        (
            "UNSORTED_WITHIN_MULTIPLE_TIMEFRAME_WITH_NANOSECONDS",
            dict(Bid=np.arange(3), Ask=np.arange(3)),
            ["2016-01-01 10:00:03", "2016-01-01 10:00:02", "2016-01-01 10:00:01"],
            [6, 5, 4],
        ),
        (
            "UNSORTED_WITHIN_MULTIPLE_TIMEFRAME_WITH_NANOSECONDS_STABLE",
            dict(Bid=np.arange(4), Ask=np.arange(4)),
            [
                "2016-01-01 10:00:03",
                "2016-01-01 10:00:03",
                "2016-01-01 10:00:02",
                "2016-01-01 10:00:02",
            ],
            [1, 1, 1, 1],
        ),
        (
            "UNSORTED_WITHIN_MULTIPLE_TIMEFRAME_WITH_NANOSECONDS_AND_DUPLICATED",
            dict(Bid=np.arange(3), Ask=np.arange(3)),
            ["2016-01-01 10:00:03", "2016-01-01 10:00:02", "2016-01-01 10:00:01"],
            [6, 5, 4],
        ),
    ],
)
def test_write_unsorted_ticks_returns_sorted_ticks(symbol, data, index, nanoseconds):
    client.destroy(tbk = f"{symbol}/1Sec/TICK")

    in_df = utils.build_dataframe(
        data,
        pd.to_datetime(index, format="%Y-%m-%d %H:%M:%S").tz_localize("utc"),
        nanoseconds=nanoseconds,
    )

    ret = write(in_df, symbol, extract_nanoseconds=nanoseconds is not None)

    print("Msg ret: {}".format(ret))

    param = pymkts.Params([symbol], "1Sec", "TICK")

    out_df = client.query(param).first().df()
    processed_out_df = utils.process_query_result(out_df, inplace=False)

    print("\ninput df")
    print(in_df)

    print("\noutput df, postprocessed")
    print(processed_out_df)
    print("\noutput df, raw")
    print(out_df)

    pd.testing.assert_frame_equal(
        in_df.sort_index(kind="merge"), processed_out_df, check_less_precise=True
    )


@pytest.mark.parametrize(
    "symbol, size, start, window",
    [
        ("RD_UNSORTED_121", 10, "2016-01-01", "4H"),
        ("RD_UNSORTED_122", 100, "2016-01-01", "4H"),
        ("RD_UNSORTED_123", 1000, "2016-01-01", "4H"),
        ("RD_UNSORTED_124", 10000, "2016-01-01", "4H"),
        ("RD_UNSORTED_131", 10, "2016-01-01", "5Sec"),
        ("RD_UNSORTED_132", 100, "2016-01-01", "5Sec"),
        ("RD_UNSORTED_133", 1000, "2016-01-01", "5Sec"),
        ("RD_UNSORTED_134", 10000, "2016-01-01", "5Sec"),
    ],
)
def test_write_unsorted_random_data(symbol, size, start, window):
    client.destroy(tbk = f"{symbol}/1Sec/TICK")

    window = pd.Timedelta(window)
    start = pd.Timestamp(start, tz="utc")
    end = start + window

    np.random.seed(42)
    random.seed(42)

    # because we expect the some leakage within 1 second due to the nanoseconds field,
    # we add some margin to data around (not exactly super close to the central data)
    pre_in_df = utils.generate_dataframe(
        size,
        start - window,
        start - pd.Timedelta("1s"),
        random_data=True,
        sort_index=False,
    )
    in_df = utils.generate_dataframe(
        size, start, end, random_data=True, sort_index=False
    )
    post_in_df = utils.generate_dataframe(
        size, end + pd.Timedelta("1s"), end + window, random_data=True, sort_index=False
    )
    write(pre_in_df, symbol, extract_nanoseconds=True)
    write(in_df, symbol, extract_nanoseconds=True)
    write(post_in_df, symbol, extract_nanoseconds=True)

    param = pymkts.Params([symbol], "1Sec", "TICK", start=start, end=end)

    out_df = client.query(param).first().df()
    processed_out_df = utils.process_query_result(out_df, inplace=False)

    print("\ninput df")
    print(in_df)
    print("\noutput df, postprocessed")
    print(processed_out_df)
    print("\noutput df, raw")
    print(out_df)

    pd.testing.assert_frame_equal(
        in_df.sort_index(kind="merge"), processed_out_df, check_less_precise=True
    )
