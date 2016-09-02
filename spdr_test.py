import pandas as pd
import numpy as np
import itertools
import multiprocessing
from pyspark import SparkContext

pd.set_option("display.width", 10000)

ohlc_dict = {
    'o': 'first',
    'h': 'max',
    'l': 'min',
    'c': 'last',
    'v': 'sum',
}


def check_combo(combo):
    for g in train.groupby(combo):
        if g[1].shape[0] >= 100:
            for symbol in symbols:
                bc = np.bincount(g[1][symbol])
                wr = float(bc[1]) / g[1].shape[0]
                cp = (g[1]["{}_pct".format(symbol)] * 1 + 1).cumprod()[-1]
                if cp > 1.05:
                    r = {"c": combo, "p": g[0], "s": symbol, "wr": wr, "len": g[1].shape[0], "cp": cp}
                    print r
                    yield r


store = pd.HDFStore("spdr.h5")

symbols = store.keys()
symbols = map(lambda s: s[1:], symbols)
symbols = filter(lambda s: s not in ["XLFS", "XLRE"], symbols)

temp = []
for symbol in symbols:
    d = store[symbol]
    d = d.resample("5T", how=ohlc_dict)
    print symbol, d.shape[0], d["v"].sum()
    temp.append(d[["c"]])

store.close()

d = pd.concat(temp, 1)
d.columns = symbols

for symbol in symbols:
    d["{}_pct".format(symbol)] = d[symbol].pct_change()
    d[symbol] = d["{}_pct".format(symbol)] > 0

offset_symbols = []
for symbol in symbols:
    for offset in xrange(1, 5):
        d["{}_{}".format(symbol, offset)] = d[symbol].shift(offset)
        offset_symbols.append("{}_{}".format(symbol, offset))

d = d.sort_index(1)

d = d.dropna()

# train = d.iloc[:400]
# test = d.iloc[400:]

train = d
test = d

print train.shape, test.shape

# print d[symbols]
# print d[offset_symbols]

# train_sel = train[(train["XLF_4"] == False) & (train["XLY_1"] == False)]
# print train.shape[0], train_sel.shape[0], (train_sel["XLE_pct"] + 1).cumprod()[-1]
# test_sel = test[(test["XLF_4"] == False) & (test["XLY_1"] == False)]
# print test.shape[0], test_sel.shape[0], (test_sel["XLE_pct"] + 1).cumprod()[-1]
#
# raise

for symbol in symbols:
    print symbol, np.bincount(d[symbol])

combos = []
for n in xrange(2, 5):
    for c in itertools.combinations(offset_symbols, n):
        combos.append(c)

sc = SparkContext()

combos_RDD = sc.parallelize(combos, multiprocessing.cpu_count() * 5)

result = combos_RDD.flatMap(check_combo).collect()

sc.stop()

result = pd.DataFrame(result)
print result.sort_values("cp")
