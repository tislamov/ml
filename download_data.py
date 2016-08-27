import os
import pandas as pd
from pandas_datareader import data
import traceback

pd.set_option("display.width", 1000)

etfs = pd.DataFrame.from_csv("ETFList.csv")

for symbol in etfs.index:
    name = etfs.loc[symbol]["Name"]
    if "bull" not in name.lower() and "bear" not in name.lower() and "ultra" not in name.lower():
        filename = "data/{}.csv".format(symbol)
        if not os.path.isfile(filename):
            try:
                d = data.DataReader(symbol, "yahoo")
                print symbol, d.shape, d.index[0]
                d.to_csv("data/{}.csv".format(symbol))
            except:
                traceback.print_exc()

temp = []
for filename in os.listdir("data"):
    symbol, _ = os.path.splitext(filename)
    d = pd.DataFrame.from_csv("data/{}".format(filename))
    print symbol, d.shape
    d[symbol] = d["Adj Close"].pct_change() + 1
    d["{}_3".format(symbol)] = d["Adj Close"].pct_change(3) + 1
    d["{}_next".format(symbol)] = d[symbol].shift(-1)
    d["{}_vol".format(symbol)] = pd.rolling_mean(d["Volume"], 60)
    d = d.dropna()
    temp.append(d[["{}_3".format(symbol), "{}_next".format(symbol), "{}_vol".format(symbol)]])
    # if len(temp) == 5:
    #     break

combined = pd.concat(temp, 1)

combined.to_csv("combined.csv")

print combined.shape
