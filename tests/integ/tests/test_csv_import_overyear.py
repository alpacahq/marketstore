"""
Integration Test for over-year CSV import
"""
import pytest
import os
import sys
import subprocess

import numpy as np
import pandas as pd

import pymarketstore as pymkts

# Constants
DATA_TYPE_TICK = [('Epoch', 'i8'), ('Bid', 'f4'), ('Ask', 'f4')]
MARKETSTORE_HOST = "localhost"
MARKETSTORE_PORT = os.getenv('MARKETSTORE_PORT', 5993)

client = pymkts.Client(f"http://{MARKETSTORE_HOST}:{MARKETSTORE_PORT}/rpc",
                       grpc=(os.getenv("USE_GRPC", "false") == "true"))


def timestamp(datestr):
    return int(pd.Timestamp(datestr).value / 10 ** 9)


@pytest.mark.parametrize('symbol, connect_option, data', [
    #('CSVT', "--url {}:{}".format(MARKETSTORE_HOST, MARKETSTORE_PORT), [(timestamp('2019-01-01 00:00:00'), 1, 2)]),
    #('CSVT2', "-d `pwd`/../testdata/mktsdb".format(MARKETSTORE_HOST, MARKETSTORE_PORT), [(timestamp('2019-01-01 01:00:00'), 3, 4)]),
    ('CSVT2', "-d `pwd`/../../../data --disable_variable_compression=True".format(MARKETSTORE_HOST, MARKETSTORE_PORT),
     [(timestamp('2019-01-01 01:00:00'), 3, 4)]),

])
def test_csv_import_over_year(symbol, connect_option, data):
    # ---- given ----
    timeframe = "1Sec"
    attribute_group = "TICK"
    tbk = f"{symbol}/{timeframe}/{attribute_group}"
    client.destroy(tbk)  # setup

    # --- write initial data ---
    client.write(np.array(data, dtype=DATA_TYPE_TICK), tbk, isvariablelength=True)

    try:
        # --- import csv ---
        command = """
marketstore connect {} <<- EOF
\load {} ../bin/example-over-year.csv ../bin/example-over-year.yaml
EOF""".format(connect_option, tbk)
        res = subprocess.run(command, shell=True, check=True, capture_output=True)
        print("STDOUT:")
        print(res.stdout.decode())
    except subprocess.CalledProcessError as e:
        print('ERROR:', e.stdout)
        assert False  # fail

    # ---- when ----
    reply = client.query(pymkts.Params(symbol, timeframe, attribute_group, limit=10))

    # ---- then ----
    df = reply.first().df()
    print(df)
    # assert additional data is inserted by the CSV import
    assert len(df.index) ==4

    # tearDown
    #client.destroy(tbk)
