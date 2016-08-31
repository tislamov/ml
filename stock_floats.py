import pandas as pd
import requests
import json
import traceback
from StringIO import StringIO

urls = [
    "http://www.nasdaq.com/screening/companies-by-industry.aspx?exchange=NASDAQ&render=download",
    "http://www.nasdaq.com/screening/companies-by-industry.aspx?exchange=NYSE&render=download",
    "http://www.nasdaq.com/screening/companies-by-industry.aspx?exchange=AMEX&render=download",
]

symbols = []
for url in urls:
    content = requests.get(url).content
    d = pd.DataFrame.from_csv(StringIO(content))
    symbols += list(d.index.values)

symbols = map(lambda s: s.strip(), symbols)

symbols = sorted(set(symbols))

symbols = filter(lambda s: (s.find("^") == -1 and s.find(".") == -1), symbols)

flts = {}

for symbol in symbols:

    url = "https://query2.finance.yahoo.com/v10/finance/quoteSummary/{}?formatted=true&crumb=f%2F19h1vPOPi&lang=en-US&region=US&modules=defaultKeyStatistics%2CfinancialData%2CcalendarEvents&corsDomain=finance.yahoo.com".format(
        symbol)

    while True:
        try:
            res = requests.get(url).json()
            break
        except:
            traceback.print_exc()

    try:

        flt = res["quoteSummary"]["result"][0]["defaultKeyStatistics"]["floatShares"]["raw"]
        print symbol, flt
        flts[symbol] = flt

    except:
        print symbol, "ERR!"

        # if len(flts) == 5:
        #     break

json.dump(flts, open("floats.json", "w"), indent=4)
print len(flts)
