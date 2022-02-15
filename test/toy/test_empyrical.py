# https://www.cnblogs.com/bitquant/p/8432891.html
import empyrical as e
import matplotlib.pyplot as plt

from datasource import datasource_factory, datasource_utils

datasource = datasource_factory.get()

df = datasource.daily(stock_code='600982.SH', start_date='20200101', end_date='20210101')
df = datasource_utils.reset_index(df,date_only=True)

print("600982.SH 20180101~20210101 :", len(df), "条:")

df_returns = df.pct_chg/100
print(df_returns)

df_returns.plot()

df_cum_returns = e.cum_returns(df_returns)
print("累计收益：", df_cum_returns.iloc[-5:])

max_drawdowns = e.max_drawdown(df_returns)
print("最大回撤：", max_drawdowns)

annual_return = e.annual_return(df_returns)
print("年化收益：", max_drawdowns)

annual_volatility = e.annual_volatility(df_returns, period='daily')
print("年化波动率", annual_volatility)

sharpe_ratio = e.sharpe_ratio(returns=df_returns)
print("夏普比率：", sharpe_ratio)

plt.plot(df_returns)
plt.plot(df_cum_returns)
plt.savefig("debug/test_empyrical.jpg")

# python -m test.toy.test_empyrical
