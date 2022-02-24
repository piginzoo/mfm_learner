import pandas as pd

from example.analysis.score import calc_monotony

"""

pytest -o log_cli=true test/unitest/test_factor_combiner.py -s

"""


def test_calc_monotony():
    df = pd.read_csv("test/data/mean_quantile_ret_bydate.csv", index_col=['factor_quantile', 'date'])
    print("原始测试数据：")
    print(df)
    returns = calc_monotony(df, periods=[1, 5, 10])
    print("单调性计算结果：>>>>>>>>>")
    print(returns)
