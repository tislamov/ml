import datetime
import pytz
import pandas as pd
import numpy as np
import requests
from sklearn.linear_model import LinearRegression


def get_yahoo(symbol):

    now = datetime.datetime(2016, 9, 1, 23, 59).replace(tzinfo=pytz.timezone("US/Eastern"))

    dt = now - datetime.timedelta(days=29)

    # print now - dt

    period2 = int((now - datetime.datetime(1970, 1, 1, tzinfo=pytz.timezone("UTC"))).total_seconds())
    period1 = int((dt - datetime.datetime(1970, 1, 1, tzinfo=pytz.timezone("UTC"))).total_seconds())

    url = "https://query1.finance.yahoo.com/v7/finance/chart/{}?period2={}&period1={}&interval=1m&indicators=quote&includeTimestamps=true&includePrePost=false&events=div%7Csplit%7Cearn&corsDomain=finance.yahoo.com".format(
        symbol,
        period2,
        period1)

    res = requests.get(url).json()

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
        rows.append({"ts": ts, "o": row_open, "h": row_high, "l": row_low, "c": row_close, "v": row_volume})

    d = pd.DataFrame(rows)
    d["dt"] = pd.to_datetime(d["ts"], unit="s")
    d.set_index("dt", inplace=True)
    d.drop("ts", 1, inplace=True)

    d = d.tz_localize("UTC").tz_convert("US/Eastern")

    return d


ohlc_dict = {
    'o': 'first',
    'h': 'max',
    'l': 'min',
    'c': 'last',
    'v': 'sum',
}

d = get_yahoo("UPRO")

d = d.resample("10T", how=ohlc_dict).dropna()

d["ret"] = (d["c"] - d["o"]) / d["o"]

d["d"] = d.index.date

samples = []
for g in d.groupby("d"):
    # print g[0], g[1].shape
    samples.append(g[1]["ret"].values)

samples = np.array(samples)

X, y = samples[:, 0:samples.shape[1]-1], samples[:, -1]
# X, y = samples[:, 0:1], samples[:, -1]

lr = LinearRegression()
lr.fit(X, y)

print lr.score(X, y)

y_pred = lr.predict(X)
# print y[y_pred > 0]
print np.cumprod(y[y_pred > 0] * 1 + 1)[-1]
# print np.cumprod(np.negative(y[y_pred < 0]) * 4 + 1)[-1]
print y.mean()
