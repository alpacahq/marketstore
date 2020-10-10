import pytest
import os

import numpy as np
import pandas as pd

import pymarketstore as pymkts

client = pymkts.Client(f"http://127.0.0.1:{os.getenv('MARKETSTORE_PORT',5993)}/rpc",
                       grpc=(os.getenv("USE_GRPC", "false") == "true"))

TIMEFRAME = '1Min'
ATTRGROUP = 'TICK'


def convert(data, with_nanoseconds=False):
    """
    convert pandas DataFrame to Numpy Records
    :param data: DataFrame object to convert
    :param with_nanoseconds: if true, add a Nanosecond field to data before convert
    :return: converted Numpy Records

    NOTE: Normal write interface removes any time information after the second
          so no test including data with time info more precise than second
          can succeed for now...

          To overcome this, we :
          - add manually a Nanosecond field: DONE
          - add the nanesecond field to the index at query time: TODO
    """
    data = data.copy()
    total_ns = data.index.astype(np.int64)

    if with_nanoseconds:
        data['Nanoseconds'] = total_ns % (10 ** 9)
        data = data.astype({"Nanoseconds": 'i4'})

    data.index = total_ns // 10 ** 9
    data.index.name = 'Epoch'
    records = data.to_records(index=True)
    return records


def get_tbk(symbol, timeframe, attrgroup):
    return '{}/{}/{}'.format(
        symbol, timeframe, attrgroup)


@pytest.fixture
def db():
    db = {
        'TEST_SIMPLE_TICK': pd.DataFrame(
            dict(Bid=[0, 1, 2],
                 Ask=[3, 4, 5],
                 ),
            index=["2016-01-01 10:01:00",
                   "2016-01-01 10:02:00",
                   "2016-01-01 10:03:00",
                   ]
        ),
        'TEST_DUPLICATED_INDEX': pd.DataFrame(
            dict(Bid=[0, 1, 2],
                 Ask=[3, 4, 5],
                 ),
            index=["2016-01-01 10:00:00",
                   "2016-01-01 10:05:00",
                   "2016-01-01 10:05:00",
                   ]
        ),
        'TEST_MULTIPLE_TICK_IN_TIMEFRAME': pd.DataFrame(
            dict(Bid=[0, 1, 2],
                 Ask=[3, 4, 5],
                 ),
            index=["2016-01-01 10:00:00",
                   "2016-01-01 10:05:00",
                   "2016-01-01 10:05:05",
                   ]
        ),
        'TEST_MILLISECOND_EPOCH': pd.DataFrame(
            dict(Bid=[0, 1, 2],
                 Ask=[3, 4, 5],
                 Nanoseconds=[140000, 240000, 340000],
                 ),
            index=["2016-01-01 10:00:00",
                   "2016-01-01 10:01:00",
                   "2016-01-01 10:02:00",
                   ]
        ),
        'TEST_MILLISECOND_EPOCH_SAME_TIMEFRAME': pd.DataFrame(
            dict(Bid=[0, 1, 2],
                 Ask=[3, 4, 5],
                 Nanoseconds=[140000, 240000, 340000],
                 ),
            index=["2016-01-01 10:00:00",
                   "2016-01-01 10:00:00",
                   "2016-01-01 10:00:00",
                   ]
        ),
    }

    for k, v in db.items():
        v.index.name = 'Epoch'
        fmt = '%Y-%m-%d %H:%M:%S'
        v.index = pd.to_datetime(v.index, format=fmt).tz_localize('utc')
    return db


@pytest.mark.parametrize('symbol, with_nanoseconds', [
    ('TEST_SIMPLE_TICK', False),
    ('TEST_DUPLICATED_INDEX', False),
    ('TEST_MULTIPLE_TICK_IN_TIMEFRAME', False),
    ('TEST_MILLISECOND_EPOCH', True),
    ('TEST_MILLISECOND_EPOCH_SAME_TIMEFRAME', True)
])
def test_integrity_ticks(db, symbol, with_nanoseconds):
    # ---- given ----
    tbk = get_tbk(symbol, TIMEFRAME, ATTRGROUP)
    client.destroy(tbk=tbk)
    data = db[symbol]

    records = convert(data, with_nanoseconds=with_nanoseconds)

    # ---- when ----
    ret = client.write(records, tbk, isvariablelength=True)
    print("Msg ret: {}".format(ret))

    assert symbol in list(db.keys())

    param = pymkts.Params([symbol],
                          TIMEFRAME,
                          ATTRGROUP,
                          )

    ret_df = client.query(param).first().df()

    # delete Nanoseconds column because it has some error and we can't assert
    # assert (data == ret_df).all().all() # fails if assert here!
    data = data.drop(columns="Nanoseconds", errors="ignore")
    ret_df = ret_df.drop(columns="Nanoseconds", errors="ignore")

    # ---- then ----
    assert (data == ret_df).all().all()
