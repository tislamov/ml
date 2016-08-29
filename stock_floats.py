import pandas as pd
import requests
import json
import traceback

stocks1 = pd.DataFrame.from_csv("NASDAQ.csv")
stocks2 = pd.DataFrame.from_csv("NYSE.csv")

stocks1.index = stocks1.index.str.strip()
stocks2.index = stocks2.index.str.strip()

symbols = sorted(set(list(stocks1.index.values) + list(stocks2.index.values)))

flts = {}

for symbol in symbols:

    url = "https://query2.finance.yahoo.com/v10/finance/quoteSummary/{}?formatted=true&crumb=f%2F19h1vPOPi&lang=en-US&region=US&modules=defaultKeyStatistics%2CfinancialData%2CcalendarEvents&corsDomain=finance.yahoo.com".format(symbol)

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
