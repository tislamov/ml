import requests
import datetime
import pytz
import lxml.html
import os
import json
import fileinput

# folder = "yahoo_cache"
# data = requests.get("http://www.cboe.com/products/snp500.aspx").content
#
# root = lxml.html.fromstring(data)
#
# symbols = []
#
# for table in root.findall(".//table"):
#     for tr in table.findall(".//tr"):
#         if tr[1].text and tr.get("height") == "20":
#             symbol = tr[1].text.strip()
#             symbols.append(symbol)
#
# print len(symbols)

# folder = "yahoo_cache.etf"
# etfs = pd.DataFrame.from_csv("ETFList.csv")
# symbols = etfs.index

folder = "yahoo_cache.russell_3000"
symbols = []
for line in fileinput.input("russell_3000"):
    symbols.append(line.strip().replace(".", "-"))
print len(symbols)


for symbol in symbols:

    print symbol

    filename = "{}/{}.json".format(folder, symbol)

    if os.path.isfile(filename):
        print "skipping..."
        continue

    while True:

        # now = datetime.datetime.utcnow().replace(tzinfo=pytz.timezone("UTC"))

        now = datetime.datetime(2016, 8, 26, 17).replace(tzinfo=pytz.timezone("US/Eastern"))

        dt = now - datetime.timedelta(days=29)

        print now - dt

        period2 = int((now - datetime.datetime(1970, 1, 1, tzinfo=pytz.timezone("UTC"))).total_seconds())
        period1 = int((dt - datetime.datetime(1970, 1, 1, tzinfo=pytz.timezone("UTC"))).total_seconds())

        url = "https://query1.finance.yahoo.com/v7/finance/chart/{}?period2={}&period1={}&interval=1m&indicators=quote&includeTimestamps=true&includePrePost=false&events=div%7Csplit%7Cearn&corsDomain=finance.yahoo.com".format(
            symbol,
            period2,
            period1)

        res = requests.get(url).json()

        assert res["chart"]["error"] is None, "!"

        if "timestamp" not in res["chart"]["result"][0]:
            break

        print len(res["chart"]["result"][0]["timestamp"])

        if len(res["chart"]["result"][0]["timestamp"]) > 100:
            json.dump(res, open(filename, "w"), indent=4)
            break

        print "trying again..."
