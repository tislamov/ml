import argparse
import datetime
import requests
import pytz
import pandas as pd
import matplotlib.pyplot as plt
from matplotlib.finance import candlestick_ohlc
from matplotlib.dates import date2num


pd.set_option("display.width", 1000)

parser = argparse.ArgumentParser()
parser.add_argument("--symbol", default="AAPL")
parser.add_argument("--date", default="now")
args = parser.parse_args()
print args.symbol

if args.date == "now":
    now = datetime.datetime.now().replace(hour=15, minute=0).replace(tzinfo=pytz.timezone("UTC"))
else:
    now = datetime.datetime.strptime(args.date, "%Y-%m-%d").replace(hour=15, minute=0).replace(tzinfo=pytz.timezone("UTC"))

start = datetime.datetime(now.year, now.month, now.day, 12, 30).replace(tzinfo=pytz.timezone("UTC"))

print now, start

period1 = int((start - datetime.datetime(1970, 1, 1, tzinfo=pytz.timezone("UTC"))).total_seconds())
period2 = int((now - datetime.datetime(1970, 1, 1, tzinfo=pytz.timezone("UTC"))).total_seconds())

print period1, period2

url = "https://query1.finance.yahoo.com/v7/finance/chart/{}?period2={}&period1={}&interval=1m&indicators=quote&includeTimestamps=true&includePrePost=true&events=div%7Csplit%7Cearn&corsDomain=finance.yahoo.com".format(
    args.symbol,
    period2,
    period1)

res = requests.get(url).json()

timestamps = res["chart"]["result"][0]["timestamp"]
open_ = res["chart"]["result"][0]["indicators"]["quote"][0]["open"]
high = res["chart"]["result"][0]["indicators"]["quote"][0]["high"]
low = res["chart"]["result"][0]["indicators"]["quote"][0]["low"]
close = res["chart"]["result"][0]["indicators"]["quote"][0]["close"]
volume = res["chart"]["result"][0]["indicators"]["quote"][0]["volume"]

data = pd.DataFrame(zip(timestamps, open_, high, low, close, volume))
data.columns = ["ts", "o", "h", "l", "c", "v"]
data["dt"] = pd.to_datetime(data["ts"], unit="s")
data.drop("ts", 1, inplace=True)
data.set_index("dt", inplace=True)
data = data.tz_localize("UTC").tz_convert("US/Eastern").tz_localize(None)

data["dt"] = data.index
data["ts"] = data["dt"].apply(date2num, 1)

print data

fig, ax = plt.subplots()
candlestick_ohlc(ax, data[["ts", "o", "h", "l", "c"]].values, width=0.0005, colorup="g")
ax.xaxis_date()
ax.autoscale_view()
plt.title("{} - {}".format(args.symbol, args.date))
plt.show()
