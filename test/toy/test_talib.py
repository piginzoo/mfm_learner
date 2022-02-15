# https://www.jianshu.com/p/f6e3caacc7cb
import talib as ta

from datasource import datasource_factory

datasource = datasource_factory.get()


def test_atr():
    df = datasource.daily(stock_code='600982.SH', start_date='20200101', end_date='20210101')
    df['atr'] = ta.ATR(df.high.values,df.low.values,df.close.values, timeperiod=14)
    print(df[10:].head(10))

# python -m test.toy.test_talib
if __name__ == '__main__':
    test_atr()
