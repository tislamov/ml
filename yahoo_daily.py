import json
import pandas as pd
import traceback
from pandas_datareader import data

flts = json.load(open("floats.json"))

store = pd.HDFStore("daily.h5")

for symbol in sorted(flts.keys()):
    if symbol not in store:
        try:
            print symbol
            d = data.DataReader(symbol, "yahoo")
            print symbol, d.shape
            if d.shape[0] > 0:
                store[symbol] = d
        except:
            traceback.print_exc()

store.close()
