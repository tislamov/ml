import json
import os
import pandas as pd

pd.set_option("display.width", 1000)

# folder, result_filename = "yahoo_cache", "sp500_1month.csv"
# folder, result_filename = "yahoo_cache.etf", "etf_1month.csv"

temp = []

for filename in os.listdir(folder):
    symbol, _ = os.path.splitext(filename)
    res = json.load(open(os.path.join(folder, filename), "r"))
    timestamps = res["chart"]["result"][0]["timestamp"]
    open_ = res["chart"]["result"][0]["indicators"]["quote"][0]["open"]
    high = res["chart"]["result"][0]["indicators"]["quote"][0]["high"]
    low = res["chart"]["result"][0]["indicators"]["quote"][0]["low"]
    close = res["chart"]["result"][0]["indicators"]["quote"][0]["close"]
    volume = res["chart"]["result"][0]["indicators"]["quote"][0]["volume"]
    print symbol, len(timestamps)

    rows = []
    for i in xrange(len(timestamps)):
        ts = timestamps[i]
        row_open = open_[i]
        row_high = high[i]
        row_low = low[i]
        row_close = close[i]
        row_volume = volume[i]
        rows.append({"ts": ts, "{}_o".format(symbol): row_open, "{}_h".format(symbol): row_high, "{}_l".format(symbol): row_low, "{}_c".format(symbol): row_close, "{}_v".format(symbol): row_volume})

    d = pd.DataFrame(rows)
    d["dt"] = pd.to_datetime(d["ts"], unit="s")
    d.set_index("dt", inplace=True)
    d.drop("ts", 1, inplace=True)

    d = d.tz_localize("UTC").tz_convert("US/Eastern")

    temp.append(d)

    # if len(temp) == 10:
    #     break

combined = pd.concat(temp, 1)
print combined
print len(temp)

combined.to_csv(result_filename)
