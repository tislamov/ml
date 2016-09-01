import json
import multiprocessing

import pandas as pd
from pyspark import SparkContext
import matplotlib.pyplot as plt
from mpl_toolkits.mplot3d import Axes3D


def process_key(item):

    pd.set_option("display.width", 10000)
    symbol, flt = item
    store = pd.HDFStore("daily.h5")
    if symbol in store:
        d = store[symbol]
        d["Next Open"] = d["Open"].shift(-1)
        d["Gap"] = (d["Next Open"] - d["Close"]) / d["Close"]
        d["Range"] = (d["High"] - d["Low"]) / d["Low"]
        yield {"symbol": symbol, "flt": flt, "gap": d["Gap"].mean(), "range": d[d["Range"] < 1]["Range"].mean(),
               "price": d.iloc[-1]["Close"]}


flts = json.load(open("floats.json"))

# flts = {"CTRV": 0}

sc = SparkContext()

keys_RDD = sc.parallelize(flts.items(), multiprocessing.cpu_count() * 5)

result = keys_RDD.flatMap(process_key).collect()

sc.stop()

result = pd.DataFrame(result)
print result.sort_values("range")

result = result[result["price"] < 100]
result = result[result["flt"] < 100000000]

fig = plt.figure()
ax = fig.add_subplot(111, projection='3d')

ax.plot(result["flt"].values, result["price"].values, result["range"].values, ".")
plt.show()
