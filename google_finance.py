import requests
import datetime
import pandas as pd
import numpy as np
from StringIO import StringIO

pd.set_option("display.width", 10000)
pd.set_option("display.max_rows", 10000)


def get_intraday_data(symbol, exchange, interval_seconds=301, num_days=10):
    # Specify URL string based on function inputs.
    url_string = 'http://www.google.com/finance/getprices?q={0}&x={1}'.format(symbol, exchange)
    url_string += "&i={0}&p={1}d&f=d,o,h,l,c,v".format(interval_seconds, num_days)

    # Request the text, and split by each line
    r = requests.get(url_string).text.split()

    # Split each line by a comma, starting at the 8th line
    r = [line.split(',') for line in r[7:]]
    r = filter(lambda row: row[0].startswith("a"), r)

    # Save data in Pandas DataFrame
    df = pd.DataFrame(r, columns=['Datetime', 'Close', 'High', 'Low', 'Open', 'Volume'])

    # Convert UNIX to Datetime format
    df['Datetime'] = df['Datetime'].apply(lambda x: datetime.datetime.utcfromtimestamp(int(x[1:])))
    df = df[['Datetime', 'Open', 'High', 'Low', 'Close', 'Volume']].set_index('Datetime')
    df = df.tz_localize("UTC").tz_convert("US/Eastern")
    df['Open'] = df['Open'].astype(np.float32)
    df['High'] = df['High'].astype(np.float32)
    df['Low'] = df['Low'].astype(np.float32)
    df['Close'] = df['Close'].astype(np.float32)
    df['Volume'] = df['Volume'].astype(np.int32)

    return df


# d = get_intraday_data(".DJI", "INDEXDJX", 1801, 10000)
# print d.index[0], d.index[-1], d.shape
# print d
# raise

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

print len(symbols)

symbols = ["INDEXDJX:.DJI", "INDEXNASDAQ:.IXIC", "INDEXSP:.INX", "INDEXFTSE:UKX", "INDEXDB:DAX", "INDEXEURO:PX1", "INDEXNIKKEI:NI225",
           "INDEXHANGSENG:HSI", "SHA:000001"]

store = pd.HDFStore("world_15min.h5")

for symbol in symbols:

    # if symbol in store:
    #     continue

    ss = symbol.split(":")
    if len(ss) == 2:
        d = get_intraday_data(ss[1], ss[0], 901, 10000)
    else:
        d = get_intraday_data(symbol, "", 901, 10000)

    print symbol, d.shape, d.index[0], d.index[-1], d.iloc[-1]["Close"]

    if d.shape[0] > 0:
        store[symbol] = d

store.close()
