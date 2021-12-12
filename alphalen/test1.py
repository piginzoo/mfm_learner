import pandas as pd
import tushare as ts
import utils
conf = utils.load_config()
ts.set_token(conf['tushare']['token'])
pro = ts.pro_api()

# 此接口获取的数据为未复权数据，回测建议使用复权数据，这里为批量获取股票数据做了简化
df = pro.daily(ts_code='000001.SZ,600982.SH', start_date='20200101', end_date='20211122')
df.index = pd.to_datetime(df['trade_date'])
df.index.name = None
df.sort_index(inplace=True)

# 多索引的因子列，第一个索引为日期，第二个索引为股票代码
assets = df.set_index([df.index, df['ts_code']], drop=True)

# column为股票代码，index为日期，值为收盘价
close = df.pivot_table(index='trade_date', columns='ts_code', values='close')
close.index = pd.to_datetime(close.index)

from alphalens.utils import get_clean_factor_and_forward_returns
from alphalens.tears import create_full_tear_sheet

# 我们是使用pct_chg因子数据预测收盘价，因此需要偏移1天，但是这里有2只股票，所以是shift(2)
ret = get_clean_factor_and_forward_returns(assets[['pct_chg']].shift(2), close)
create_full_tear_sheet(ret, long_short=False)

"""
python -m alphalen.test1
"""