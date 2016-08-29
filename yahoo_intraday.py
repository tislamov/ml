import datetime
import os
import json

import requests
import pytz

folder = "yahoo_cache"

floats = json.load(open("floats.json"))

symbols = sorted(floats.keys())

print symbols
print len(symbols)

for symbol in symbols:

    print symbol

    filename = "{}/{}.json".format(folder, symbol)

    if os.path.isfile(filename):
        print "skipping..."
        continue

    while True:

        now = datetime.datetime(2016, 8, 26, 17).replace(tzinfo=pytz.timezone("US/Eastern"))

        dt = now - datetime.timedelta(days=28)

        print now - dt

        period2 = int((now - datetime.datetime(1970, 1, 1, tzinfo=pytz.timezone("UTC"))).total_seconds())
        period1 = int((dt - datetime.datetime(1970, 1, 1, tzinfo=pytz.timezone("UTC"))).total_seconds())

        url = "https://query1.finance.yahoo.com/v7/finance/chart/{}?period2={}&period1={}&interval=1m&indicators=quote&includeTimestamps=true&includePrePost=true&events=div%7Csplit%7Cearn&corsDomain=finance.yahoo.com".format(
            symbol,
            period2,
            period1)

        res = requests.get(url).json()

        assert res["chart"]["error"] is None, "!"

        if len(res["chart"]["result"]) == 0:
            break

        if "timestamp" not in res["chart"]["result"][0]:
            break

        print len(res["chart"]["result"][0]["timestamp"])

        if len(res["chart"]["result"][0]["timestamp"]) > 100:
            json.dump(res, open(filename, "w"), indent=4)
            break

        print "trying again..."
