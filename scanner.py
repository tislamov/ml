import json
import pandas as pd
import numpy as np

flts = json.load(open("floats.json"))

# symbols = map(lambda s: s[0], filter(lambda s: s[1] < 10000000, flts.items()))

d = pd.read_pickle("1month.pkl")

symbols = map(lambda c: c.split("_")[0], filter(lambda c: c.endswith("_c"), d.columns.values))

close = d.between_time("15:59", "15:59")[map(lambda s: "{}_c".format(s), symbols)]
close.index = close.index.date

pre_d = d.between_time("09:00", "09:29")[map(lambda s: "{}_h".format(s), symbols)]

pre_d["d"] = pre_d.index.date

dates = close.index.values

print dates

for g in pre_d.groupby("d"):
    dp = np.where(dates == g[0])[0][0]

    if dp > 0:
        prev_dt = dates[dp - 1]
        print g[0], g[1].shape, "prev_dt", prev_dt

        for symbol in symbols:
            prev_close = close.loc[prev_dt, "{}_c".format(symbol)]
            if type(prev_close) == np.float16 and ~np.isnan(prev_close):
                cur_high = g[1]["{}_h".format(symbol)].dropna()
                if cur_high.shape[0] > 0:
                    cur_high_mean = cur_high.max()
                    if cur_high_mean >= prev_close * 1.04:
                        print symbol, prev_close, cur_high_mean, cur_high.shape, "{}M".format(int(flts[symbol] / 1000000))
