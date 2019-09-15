import numpy as np
import pandas as pd
import pymarketstore as pymkts
from datetime import datetime, timezone
import os

def test_driver():
    data = np.array([(pd.Timestamp('2017-01-01 00:00').value / 10**9, 10.0)], dtype=[('Epoch', 'i8'), ('Ask', 'f4')])

    host = os.environ.get('MARKETSTORE_HOST', 'localhost:5993')
    cli = pymkts.Client('http://{}/rpc'.format(host))

    cli.write(data, 'TEST/1Min/Tick')

    d2 = cli.query(pymkts.Params('TEST', '1Min', 'Tick')).first().df()
    print("Length of result: ",d2.shape[0])
    assert d2.shape[0] == 1
    assert datetime(2017,1,1,0,0,0,tzinfo=timezone.utc).timestamp() == d2.index[0].timestamp()
