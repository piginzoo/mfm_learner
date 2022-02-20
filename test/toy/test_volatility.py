
import numpy as np
import pandas as pd


def invest_stat(closes):
    print("收盘价\n",closes)

    returns = closes.pct_change()
    print("收益率\n",returns)

    volatilities = returns.std()
    print("波动率",volatilities)

invest_stat(pd.Series(list(range(1,100)))+100) # [101,102,...199]
print("-"*80)
invest_stat(pd.Series(list(range(1,100)))) # [1,2,...99]
print("-"*80)
invest_stat(pd.Series([1,1.1,0.9,1.1,0.9,1.1,0.9,1.1,0.9]))
print("-"*80)
invest_stat(pd.Series([np.power(1.01,x) for x in range(1,100)]))
print("-"*80)
invest_stat(pd.Series([0 - np.power(1.01,x) for x in range(1,100)]))

# python -m test.toy.test_volatility
