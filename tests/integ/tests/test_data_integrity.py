"""
STATUS:OK/NOK
STATUS:OK pymkts can write nanoseconds and query correctly
BUG: when writing with csv_writer on the 1Sec and 1Min timeframe, you can only write microseconds and it will return Nanoseconds. If you write nanoseconds, the nanoseconds will not be parsed and the returned Nanoseconds field will be 0.
MIGRATION_STATUS:OK -> `test_csv_writer` is marked as XFAIL and skipped when `marketstore` is not accessible.
"""
import pytest
import random
import numpy as np
import pandas as pd
from subprocess import run, PIPE
import time
import os
import pymarketstore as pymkts
import pathlib
from . import utils
from shutil import which


use_grpc = os.getenv("USE_GRPC", "false") == "true"
client = pymkts.Client(f"http://127.0.0.1:{os.getenv('MARKETSTORE_PORT',5993)}/rpc",
                       grpc=(use_grpc))

pathlib.Path("/tmp/test_intermediate_data").mkdir(exist_ok=True)


@pytest.mark.parametrize(
    "symbol,timeframe,size,start,window",
    [
        ("PYW_TEST_01VA", "1Min", 400, "2016-01-01 10:00:00", "5s"),
        ("PYW_TEST_02VA", "1Min", 800, "2016-01-01 10:00:00", "5s"),
        ("PYW_TEST_03VA", "1Sec", 400, "2016-01-01 10:00:00", "5s"),
        ("PYW_TEST_04VA", "1Sec", 800, "2016-01-01 10:00:00", "5s"),
        # ("PYW_TEST_05VB", "1Min", 50000, "2016-01-01 10:00:00", "4H"),
        # ("PYW_TEST_06VB", "1Min", 100_000, "2016-01-01 10:00:00", "4H"),
        # ("PYW_TEST_07VB", "1Min", 500_000, "2016-01-01 10:00:00", "4H"),
        # ("PYW_TEST_08VB", "1Min", 1_000_000, "2016-01-01 10:00:00", "4H"),
        # ("PYW_TEST_09VB", "1Sec", 50000, "2016-01-01 10:00:00", "4H"),
        # ("PYW_TEST_10VB", "1Sec", 100_000, "2016-01-01 10:00:00", "4H"),
        # ("PYW_TEST_11VB", "1Sec", 500_000, "2016-01-01 10:00:00", "4H"),
        # ("PYW_TEST_12VB", "1Sec", 1_000_000, "2016-01-01 10:00:00", "4H"),
    ],
)
def test_write_pymkts(symbol, timeframe, size, start, window):
    db_symbol = symbol + "_PY"
    client.destroy(tbk=f"{db_symbol}/{timeframe}/TICK")

    window = pd.Timedelta(window)
    start = pd.Timestamp(start, tz="utc")
    end = start + window
    np.random.seed(42)
    random.seed(42)

    df = utils.generate_dataframe(size, start, end, random_data=False)

    result = write_with_pymkts(df.copy(), db_symbol, timeframe)

    # In case of grpc, a MultiServerResponse is returned and need to be handled differently.
    if not use_grpc:
        assert result["responses"] is None

    assert_query_result(df, db_symbol, size, timeframe, start, end)


def write_with_pymkts(df: pd.DataFrame, symbol: str, timeframe: str):
    """
        Write with pymarketstore client: function to benchmark
    """
    records = utils.to_records(df, extract_nanoseconds=True)
    tbk = f"{symbol}/{timeframe}/TICK"
    return client.write(records, tbk, isvariablelength=True)


@pytest.mark.parametrize(
    "symbol,timeframe,size,start,window,format",
    [
        # (dakimura, 2020-08-02, let me comment out this test for now as it needs some refactor...)
        # ("CSVW_TEST_01VA", "1Min", 400, "2016-01-01 10:00:00", "5s", "ms"),
        # ("CSVW_TEST_02VA", "1Min", 800, "2016-01-01 10:00:00", "5s", "ms"),
        # ("CSVW_TEST_03VA", "1Sec", 400, "2016-01-01 10:00:00", "5s", "ms"),
        # ("CSVW_TEST_04VA", "1Sec", 800, "2016-01-01 10:00:00", "5s", "ms"),
        # ("CSVW_TEST_05VA", "1Min", 50000, "2016-01-01 10:00:00", "4H", "ms"),
        # ("CSVW_TEST_06VA", "1Min", 100_000, "2016-01-01 10:00:00", "4H", "ms"),
        # ("CSVW_TEST_07VA", "1Min", 500_000, "2016-01-01 10:00:00", "4H", "ms"),
        # ("CSVW_TEST_08VA", "1Min", 1_000_000, "2016-01-01 10:00:00", "4H", "ms"),
        # ("CSVW_TEST_09VA", "1Sec", 50000, "2016-01-01 10:00:00", "4H", "ms"),
        # ("CSVW_TEST_10VA", "1Sec", 100_000, "2016-01-01 10:00:00", "4H", "ms"),
        # ("CSVW_TEST_11VA", "1Sec", 500_000, "2016-01-01 10:00:00", "4H", "ms"),
        # ("CSVW_TEST_12VA", "1Sec", 1_000_000, "2016-01-01 10:00:00", "4H", "ms"),
        # ("CSVW_TEST_01VB", "1Min", 400, "2016-01-01 10:00:00", "5s", "ns"),
        # ("CSVW_TEST_02VB", "1Min", 800, "2016-01-01 10:00:00", "5s", "ns"),
        # ("CSVW_TEST_03VB", "1Sec", 400, "2016-01-01 10:00:00", "5s", "ns"),
        # ("CSVW_TEST_04VB", "1Sec", 800, "2016-01-01 10:00:00", "5s", "ns"),
        # ("CSVW_TEST_05VB", "1Min", 50000, "2016-01-01 10:00:00", "4H", "ns"),
        # ("CSVW_TEST_06VB", "1Min", 100_000, "2016-01-01 10:00:00", "4H", "ns"),
        # ("CSVW_TEST_07VB", "1Min", 500_000, "2016-01-01 10:00:00", "4H", "ns"),
        # ("CSVW_TEST_08VB", "1Min", 1_000_000, "2016-01-01 10:00:00", "4H", "ns"),
        # ("CSVW_TEST_09VB", "1Sec", 50000, "2016-01-01 10:00:00", "4H", "ns"),
        # ("CSVW_TEST_10VB", "1Sec", 100_000, "2016-01-01 10:00:00", "4H", "ns"),
        # ("CSVW_TEST_11VB", "1Sec", 500_000, "2016-01-01 10:00:00", "4H", "ns"),
        # ("CSVW_TEST_12VB", "1Sec", 1_000_000, "2016-01-01 10:00:00", "4H", "ns"),
    ],
)
def test_csv_writer(symbol, timeframe, size, start, window, format):
    """
    All tests outputting to format='ns' fail (even in 1Min timeframe, the
    testing condition is just relaxed because of the issue with nanoseconds precision)
    """

    if not is_marketstore_client_available():
        pytest.xfail("Marketstore client is not available, the test will fail.")

    db_symbol = symbol + "_CSV"
    client.destroy(tbk="{db_symbol}/{timeframe}/TICK")

    window = pd.Timedelta(window)
    start = pd.Timestamp(start, tz="utc")
    end = start + window
    np.random.seed(42)
    random.seed(42)

    df = utils.generate_dataframe(size, start, end, random_data=False)
    # remove the nanoseconds to milliseconds
    if format == "ms":
        total_ns = df.index.astype("i8")
        df.index = pd.to_datetime(total_ns // 10 ** 3, unit="us", utc=True)

    result = write_with_csv_writer_from_filename(df, db_symbol, timeframe, format)
    print(result)

    assert f"Read next {size} lines from CSV file" in result

    # NOTE: the 1st time you write a symbol with csv_writer, you need to restart it to
    # make it queryable
    restart_marketstore()

    assert_query_result(df, db_symbol, size, timeframe, start, end)


def write_with_csv_writer_from_filename(df, symbol, timeframe, format):
    csv_filename = f"/tmp/test_intermediate_data/{symbol}.csv"

    if format == "ns":
        # BUG: for some reason with pandas when formatting with "%Y%m%d %H:%M:%S %f"
        # it only output up to microseconds and not nanoseconds...
        str_dates = df.index.strftime("%Y%m%d %H:%M:%S")
        str_ns = ["{:09d}".format(el) for el in (df.index.astype("i8") % (10 ** 9))]
        formatted_df = df.copy()
        formatted_df["Epoch"] = str_dates + " " + str_ns
        formatted_df[["Epoch", "Ask", "Bid"]].to_csv(
            csv_filename, header=True, index=False
        )
        tmpdf = pd.read_csv(csv_filename, dtype=str).head()
        assert len(tmpdf["Epoch"].iloc[0].split(" ")[-1]) == 9

    elif format == "ms":
        df[["Ask", "Bid"]].to_csv(
            csv_filename, header=True, date_format="%Y%m%d %H:%M:%S %f"
        )
        tmpdf = pd.read_csv(csv_filename, dtype=str).head()
        assert len(tmpdf["Epoch"].iloc[0].split(" ")[-1]) == 6

    print(df.head().index.values)

    print(df.head().values)
    print(tmpdf.head().values)

    config = 'firstRowHasColumnNames: true\ntimeFormat: "20060102 15:04:05"'
    config_filename = f"/tmp/test_intermediate_data/{symbol}.yaml"
    with open(config_filename, "w") as fp:
        fp.write(config)

    input = (
        f"\create {symbol}/{timeframe}/TICK:Symbol/Timeframe/AttributeGroup Bid,Ask/float32 variable\n"  # noqa
        # f"\getinfo {symbol}/{timeframe}/TICK\n"
        f"\load {symbol}/{timeframe}/TICK {csv_filename} {config_filename}\n"
        # f"\show {symbol}/{timeframe}/TICK 1970-01-01\n"
        # f"\getinfo {symbol}/{timeframe}/TICK\n"
        # "\o test_ticks.csv\n" ?
    )

    cmd = ["marketstore", "connect", "-d", "/tmp/mktsdb"]
    p = run(cmd, stdout=PIPE, input=input, encoding="ascii")
    assert p.returncode == 0
    print(p.stdout)
    return p.stdout


def restart_marketstore():
    cmd = ["pkill", "marketstore"]
    p = run(cmd, stdout=PIPE, encoding="ascii")
    time.sleep(3)
    assert p.returncode == 0
    print(p.stdout)
    return p.stdout


def assert_query_result(df, symbol, size, timeframe, start, end):
    start = pd.Timestamp(start).tz_convert("utc")
    end = pd.Timestamp(end).tz_convert("utc")

    param = pymkts.Params([symbol], timeframe, "TICK", start=start, end=end)
    out_df = client.query(param).first().df()

    processed_out_df = utils.process_query_result(out_df, inplace=False)

    assert not out_df.empty
    assert size == len(out_df)
    assert out_df.index.is_monotonic_increasing

    try:

        if timeframe == "1Sec":
            pd.testing.assert_frame_equal(df, processed_out_df, check_less_precise=True)
        else:
            # remove all nanoseconds information for now because of the precision issue
            # on nanoseconds
            # BUG
            # though whe nwe look at the raw value of the nanoseconds returned, we can
            # see it has the same issue as for 1sec: it is set to 0 which is then
            # flipped to  99999999xx due to precision issue

            pd.testing.assert_frame_equal(
                rm_ns_from_idx(df),
                rm_ns_from_idx(processed_out_df),
                # check_less_precise=True,
                # commented out as the warning shown:
                # FutureWarning: The 'check_less_precise' keyword in testing.assert_*_equal is deprecated and will be removed
                # in a future version. You can stop passing 'check_less_precise' to silence this warning. check_less_precise=True,
            )

    except AssertionError:

        if len(df) != len(out_df):
            print("lengths do not match, inspect manually")
            raise

        bad_locations = df.index != processed_out_df.index
        dilated_bad_locations = np.convolve(
            bad_locations.astype(int), [1, 1, 1], mode="same"
        ).astype(bool)
        print("Show dilated bad locations".center(40, "-"))
        print("\ninput df")
        print(df.loc[dilated_bad_locations, :])
        print("\noutput df, postprocessed")
        print(processed_out_df.loc[dilated_bad_locations, :])
        print("\noutput df, raw")
        print(out_df.loc[dilated_bad_locations, :])

        raise


def rm_ns_from_idx(df):
    df = df.copy()
    df.index = pd.to_datetime(
        df.index.strftime("%Y%m%d %H:%M:%S"), utc=True
    )
    return df

def is_marketstore_client_available():
    """Check whether `marketstore` command line client is in the PATH and executable."""

    return which('marketstore') is not None
