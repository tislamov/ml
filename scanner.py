import json
import pandas as pd
import multiprocessing
from pyspark import SparkContext
import matplotlib.pyplot as plt

pd.set_option("display.max_rows", 10000)


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

            if pre_d.shape[0] > 0:
                if 1 < prev_c < 10 and flt < 20e6:
                    r = {"symbol": symbol, "flt": flt, "prev_close": prev_c, "pre_d_shape": pre_d.shape[0],
                         "d": g[0], "pre_high": pre_d["h"].max()}
                    print r
                    yield r


flts = json.load(open("floats.json"))

sc = SparkContext()

items_RDD = sc.parallelize(flts.items(), multiprocessing.cpu_count() * 5)

result = items_RDD.flatMap(process_item).collect()

sc.stop()

result = pd.DataFrame(result)
result.to_pickle("scanner.pkl")
print result

result = pd.read_pickle("scanner.pkl")

result["gap"] = (result["pre_high"] - result["prev_close"]) / result["prev_close"]
print result["gap"].mean(), result["gap"].median(), result["gap"].max()
result = result[(result["pre_d_shape"] > 3) & (result["gap"] > .04)].sort_values("d")

print result
print result.shape
