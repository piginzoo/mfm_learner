import pandas as pd


def make_dummy_data():
    data1 = [
        ['000001.SZ', '2016-06-24', 0.165260, 0.002198, 0.085632, -0.078074, 0.173832, 0.214377, 0.068445],
        ['000001.SZ', '2016-06-27', 0.165537, 0.003583, 0.063299, -0.048674, 0.180890, 0.202724, 0.081748],
        ['000001.SZ', '2016-06-28', 0.135215, 0.010403, 0.059038, -0.034879, 0.111691, 0.122554, 0.042489],
        ['000002.SH', '2016-06-24', 0.068774, 0.019848, 0.058476, -0.049971, 0.042805, 0.053339, 0.079592],
        ['000002.SH', '2016-06-27', 0.039431, 0.012271, 0.037432, -0.027272, 0.010902, 0.077293, -0.050667],
        ['000002.SH', '2016-06-28', 0.039431, 0.012271, 0.037432, -0.027272, 0.010902, 0.077293, -0.050667],
        ['000002.SH', '2016-06-29', 0.039431, 0.012271, 0.037432, -0.027272, 0.010902, 0.077293, -0.050667]
    ]
    data1 = pd.DataFrame(data1,
                         columns=["code", "datetime", "BP", "CFP", "EP", "ILLIQUIDITY", "REVS20", "SRMI", "VOL20"])
    data1['datetime'] = pd.to_datetime(data1['datetime'], format='%Y-%m-%d')  # 时间为日期格式，tushare是str
    data1 = data1.set_index(["datetime"])
    return data1


# https://blog.csdn.net/maymay_/article/details/80241627
# https://zhuanlan.zhihu.com/p/91100281
def test_rolling():
    s = [1, 2, 3, 5, 6, 10, 12, 14, 12, 30]
    s1 = pd.Series(s).rolling(window=3).mean()
    df = pd.DataFrame([s,s1])
    print("rolling(window=3).mean():")
    print(df)
    print("-"*80)
    df = make_dummy_data()
    print("before rolling:")
    print(df)
    df = df.rolling(window=3).mean()
    print("after rolling:")
    print(df)

# https://blog.csdn.net/wangshuang1631/article/details/52314944
def test_resample():
    df = make_dummy_data()
    print(df)
    df_s = df[['EP', 'BP']].resample('3D').agg(['min', 'max', 'sum'])
    print(df_s)
    print(df.index.freq,df_s.index.freq)
    df.index.freq = df_s.index.freq

# python -m test.toy.test_pandas
if __name__ == '__main__':
    test_rolling()
    test_resample()
