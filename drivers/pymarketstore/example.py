from pymarketstore import client as pmkts
import numpy as np
import traceback

# Initalize the connection to the database
# Must be formatted as <address>:<port>
c = pmkts.Client("127.0.0.1:5992")

# Create a parameter object to query
# Symbol, time frame, attribute group, start time, end time
p = pmkts.Params("TSLA", "1Min", "OHLCV", 1500000000, 4294967296)
p_lst = [p]

p2 = pmkts.Params("NVDA", "1Min", "OHLCV", 1497484800, 4294967296)
p_lst.append(p2)

# Query takes a list of parameter objects
# Returns a dictionary of <symbol>/<time frame>/<attribute group>
# The values are pandas dataframes
resp = c.query(p_lst)

print(resp['TSLA/1Min/OHLCV'])
print(resp['NVDA/1Min/OHLCV'])

# To Query multiple symbols for a given timeframe
# Append on a symbol
p = pmkts.Params("NVDA", "1Min", "OHLCV", 1497484800, 4294967296)
p.append_symbol("TSLA")

# Mix and match Params objects for multi-dataset queries
p2 = pmkts.Params("NVDA", "5Min", "OHLCV", 1497484800, 4294967296)

resp = c.query([p, p2])

print(resp['TSLA/1Min/OHLCV'])
print(resp['NVDA/1Min/OHLCV'])
print(resp['NVDA/5Min/OHLCV'])

# Write to the database
# This object expects a numpy ndarray and a time bucket key
length = 2
dt = [('Epoch', '<i8', (length,)), ('Open', '<f4', (length,)), ('High', '<f4', (length,)), ('Low', '<f4', (length,)), ('Close', '<f4', (length,)), ('Volume', '<i4', (length,))]
dt = np.dtype(dt)
arr = np.empty([1, ], dtype=dt)
arr['Epoch'] = [2000000000, 2500000000]
arr['Open'] = [152.369995, 152.339996]
arr['High'] = [152.369995, 152.539993]
arr['Low'] = [152.369995, 152.119995]
arr['Close'] = [152.369995, 152.369995]
arr['Volume'] = [215383, 466322]
tbk = "TSLA/1Min/OHLCV:Symbol/Timeframe/AttributeGroup"

# Response from write will be None on success (resp['responses'] = None)
# If the write operation fails the error will be contained in
# the return value
try:
    resp = c.write(arr, tbk)
    print(resp)
except Exception as e:
    traceback.format_exc()

# Read the data back and confirm write
p = pmkts.Params("TSLA", "1Min", "OHLCV", 1500000000, 4294967296)
resp = c.query([p])
print(resp['TSLA/1Min/OHLCV'])
