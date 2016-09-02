import json
import pandas as pd
import numpy as np
import multiprocessing
from pyspark import SparkContext
import matplotlib.pyplot as plt

pd.set_option("display.max_rows", 10000)
pd.set_option("display.width", 10000)


def process_item(item):
    symbol, flt = item

    if symbol not in pd.HDFStore("daily.h5"):
        return

    daily = pd.read_hdf("daily.h5", symbol)

    d = pd.read_hdf("august.h5", symbol)

    d["d"] = d.index.date

    for g in d.groupby("d"):
        # get previous day close
        daily_before = daily[:g[0] - pd.Timedelta(days=1)]
        if daily_before.shape[0] > 0:
            prev_c = daily_before.iloc[-1]["Close"]
            pre_d = g[1].between_time("09:00", "09:29")
            first_bar = g[1].between_time("09:30", "09:30")
            if pre_d.shape[0] > 0 and first_bar.shape[0] == 1:
                if 1 < prev_c < 10 and flt < 20e6:
                    first_bar_roc = (first_bar.iloc[0]["c"] - first_bar.iloc[0]["o"]) / first_bar.iloc[0]["o"]
                    if ~np.isnan(first_bar_roc):
                        r = {"symbol": symbol, "flt": flt, "prev_close": prev_c, "pre_d_shape": pre_d.shape[0],
                             "d": g[0], "pre_high": pre_d["h"].max(), "first_bar_roc": first_bar_roc}
                        print r
                        yield r


flts = json.load(open("floats.json"))

sc = SparkContext()

items_RDD = sc.parallelize(flts.items(), multiprocessing.cpu_count() * 5)

result = items_RDD.flatMap(process_item).collect()

sc.stop()

result = pd.DataFrame(result)
result.to_pickle("scanner.pkl")
print result.sort_values("first_bar_roc")

result = pd.read_pickle("scanner.pkl")
print result.columns.values

plt.plot(result["prev_close"].values, result["first_bar_roc"].values, ".")
plt.show()
