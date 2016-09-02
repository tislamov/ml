import requests
import datetime
import pandas as pd
import numpy as np

pd.set_option("display.width", 10000)
pd.set_option("display.max_rows", 10000)


def get_intraday_data(symbol, interval_seconds=301, num_days=10):
    # Specify URL string based on function inputs.
    url_string = 'http://www.google.com/finance/getprices?q={0}'.format(symbol.upper())
    url_string += "&i={0}&p={1}d&f=d,o,h,l,c,v".format(interval_seconds, num_days)

    # Request the text, and split by each line
    r = requests.get(url_string).text.split()

    # Split each line by a comma, starting at the 8th line
    r = [line.split(',') for line in r[7:]]

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


d = get_intraday_data("SPU", 61, 1)

print d.index[0], d.index[-1]

print d
