import tushare
import numpy as np
import utils

conf = utils.load_config()
pro = tushare.pro_api(conf['tushare']['token'])


def get_last_day():
    # https://zhuanlan.zhihu.com/p/96888358
    df = pro.monthly(ts_code='000001.SZ', start_date='20081001', end_date='20201231', fields='ts_code,trade_date,open,high,low,close,vol,amount')
    adj_close = (df['close'] + df['high'] + df['low']) / 3
    return np.log(adj_close / adj_close.shift(1)) # shift(1) 往后移，就变成上个月的了


# python -m test.test_tushare
if __name__ == '__main__':
    print(get_last_day())
