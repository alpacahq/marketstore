# -*- coding:utf-8 -*-

import pandas as pd
import numpy as np
import random


def build_dataframe(data, index, columns=None, nanoseconds=None) -> pd.DataFrame:
    """Util to build a dataframe from parameterized arguments.

    Args:
        data        : `data` argument passed to pd.DataFrame(index=...)
        index       : `index` argument passed to pd.DataFrame(index=...)
        columns     : `columns` argument passed to pd.DataFrame(columns=...)
        nanoseconds : None or an array|list of nanoseconds. If provided, it will be
            added to the TimeIndex.

    Returns:
        A DataFrame build from index, data and nanoseconds specified.
    """

    df = pd.DataFrame(data=data, index=index, columns=columns)

    if not isinstance(index, pd.DatetimeIndex):
        raise ValueError("Index is not DataTimeIndex")

    if nanoseconds is not None:
        df.index = pd.to_datetime(
            df.index.values.astype("datetime64[s]")
            + np.array(nanoseconds).astype("timedelta64[ns]"),
            utc=True,
        )

    df.index.name = "Epoch"
    return df


def to_records(df: pd.DataFrame, extract_nanoseconds: bool = True) -> np.recarray:
    """Converts a dataframe to a format suitable to be written to the marketstore.

    The timeindex will be transformed to a field named `Epoch` and in seconds.
    If a field named `Nanoseconds` is present in the dataframe, it will be converted to
    int32 and generated in the records under the same name.

    Args:
        df                  : A DataFrame to be converted.
        extract_nanoseconds : If True, the Nanoseconds field will be inferred from the
            timeindex.

    Returns:
        Data in a suitable format to write with pymarketstore client.
    """
    df = df.copy()
    total_ns = df.index.astype("i8")

    if extract_nanoseconds:
        df["Nanoseconds"] = total_ns % (10 ** 9)

    if "Nanoseconds" in df.columns:
        df.loc[:, "Nanoseconds"] = df["Nanoseconds"].astype("i4")

    df.index = total_ns // (10 ** 9)
    df.index.name = "Epoch"

    records = df.to_records(index=True)
    return records


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

    # df.sort_index(axis=1, inplace=True)

    return df


def generate_dataframe(
        size: int, start: pd.Timestamp, end: pd.Timestamp, random_data: bool = True, sort_index: bool = True
) -> pd.DataFrame:
    """Generate dataframe for testing purposes.

    To be closer to real-life tick data, we provide several options and generate random
    index with 20% duplicated timestamps at nanosecond precision.

    Args:
        size        : The length of generated dataframe.
        start       : The data generated will be after this date.
        end         : The data generated will be before this date.
        random_data : If `True`, the data will be randomly generated, else it
            will be monotonous increasing (easier to inspect).
        sort_index  : If `True`, the data will not be sorted

    Returns:
        df: The generated dataframe.

    """
    if random_data:
        data = dict(
            Bid=np.random.RandomState(0).random(size=size).astype(np.float32),
            Ask=np.random.RandomState(0).random(size=size).astype(np.float32),
        )
    else:
        data = dict(
            Bid=np.arange(size).astype(np.float32),
            Ask=np.arange(size).astype(np.float32),
        )

    start_ts = pd.Timestamp(start).value
    end_ts = pd.Timestamp(end).value

    sampled_index_ts = [
        random.randint(start_ts, end_ts) for i in range(int(0.80 * size))
    ]

    duplicated_index_ts = [
        random.choice(sampled_index_ts) for i in range(int(0.2 * size))
    ]

    merged_index_ts = np.array(sampled_index_ts + duplicated_index_ts)[:size]

    if sort_index:
        merged_index_ts.sort(kind="mergesort")

    index = pd.to_datetime(merged_index_ts // (10 ** 9), unit="s")
    nanoseconds = merged_index_ts % (10 ** 9)
    df = build_dataframe(data, index, columns=["Bid", "Ask"], nanoseconds=nanoseconds)
    df.sort_index(axis=1, inplace=True)
    return df
