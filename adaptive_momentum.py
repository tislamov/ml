import pandas as pd
import itertools
import multiprocessing
import numpy as np
from pyspark import SparkContext
import matplotlib.pyplot as plt


def sort_row(row):
    return map(lambda c: c.split("_")[0], row.sort_values(ascending=False).index[:30].values)


def process_combination(c):

    ret_3_c = ret_3[map(lambda col: "{}_3".format(col), list(c))].values
    next_c = next[map(lambda col: "{}_next".format(col), list(c))].values

    sel_col = np.argmax(ret_3_c, 1)

    next_sel_col = np.column_stack((next_c, sel_col))

    ret_c = np.apply_along_axis(lambda row: row[int(row[2])], 1, next_sel_col)
    c_ret = ret_c.cumprod()[-1]

    assert not np.isnan(c_ret), ret_c.cumprod()

    yield {"c": c, "r": c_ret}


combined = pd.DataFrame.from_csv("combined.csv")

vol_columns = filter(lambda c: c.endswith("_vol"), combined.columns.values)

combined["vol_sorted"] = combined[vol_columns].apply(sort_row, 1)

total = 0

returns = []

for idx, row in combined.iterrows():
    if total >= 60:
        symbols = row["vol_sorted"]
        cols_ret_3 = map(lambda c: "{}_3".format(c), symbols)
        cols_next = map(lambda c: "{}_next".format(c), symbols)

        ret_3 = combined.iloc[total - 60: total][cols_ret_3]
        next = combined.iloc[total - 60: total][cols_next]

        combinations = []
        for n in xrange(2, 3):
            for c in itertools.combinations(symbols, n):
                combinations.append(c)

        sc = SparkContext()

        result_RDD = sc.parallelize(combinations, multiprocessing.cpu_count() * 5).flatMap(process_combination)
        result = result_RDD.collect()

        sc.stop()

        result.sort(key=lambda r: r["r"], reverse=True)

        selected_c = result[0]["c"]

        sel_cols_ret_3 = map(lambda c: "{}_3".format(c), list(selected_c))
        sel_cols_next = map(lambda c: "{}_next".format(c), list(selected_c))

        sel_symbol = row[sel_cols_ret_3].argmax().split("_")[0]

        sel_return = row["{}_next".format(sel_symbol)]

        returns.append(sel_return)

        print idx, len(combinations), result[0], sel_return, np.array(returns).cumprod()[-1]

        # if len(returns) == 10:
        #     break

    total += 1

returns = np.array(returns)
print returns
print returns.cumprod()[-1]

plt.plot(returns.cumprod())
plt.show()
