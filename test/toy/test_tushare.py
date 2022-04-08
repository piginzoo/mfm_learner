import tushare

pro = tushare.pro_api()


def get_last_day():
    # https://zhuanlan.zhihu.com/p/96888358
    df = pro.daily(ts_code='000001.SZ', start_date='20201221', end_date='20201231')#, fields='ts_code,trade_date,open,high,low,close,vol,amount')
    df.to_csv("debug/test.csv")
    return df
    # adj_close = (df['close'] + df['high'] + df['low']) / 3
    # return np.log(adj_close / adj_close.shift(1)) # shift(1) 往后移，就变成上个月的了

def get_index_weight():
    df = pro.index_weight(index_code='000300.SH', start_date='20210101', end_date='20211201')
    df.to_csv("debug/index.csv")
    return df

def test_rolling():
    import pandas as pd
    import numpy as np
    data = []
    for i in range(20):
        data.append(['ts1',i])
    for i in range(20):
        data.append(['ts2',i])
    data[10] = ['ts1',np.nan]
    data[11] = ['ts1', np.nan]
    data[12] = ['ts1', np.nan]

    df = pd.DataFrame(data,columns=['ts_code','pct_chg'])
    data = df.groupby('ts_code').pct_chg.rolling(2,min_periods=1).std(skipna = True)
    print(data)



# python -m test.toy.test_tushare
if __name__ == '__main__':
    # df = get_last_day()
    # print(df)
    # print(df.info())
    test_rolling()

