# 参考：https://jishuin.proginn.com/p/763bfbd6cf7d
import pandas as pd
import tushare as ts
from alphalens.tears import create_returns_tear_sheet, create_information_tear_sheet, \
    create_turnover_tear_sheet
from alphalens.tears import plotting
from alphalens.utils import get_clean_factor_and_forward_returns

from mfm_learner import utils

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

# 我们是使用pct_chg因子数据预测收盘价，因此需要偏移1天，但是这里有2只股票，所以是shift(2)
# 将 因子数据、价格数据以及行业分类按照索引对齐地格式化到一个数据表中，这个数据表的索引是包含日期和资产的多重索引”，
# 我们理解就是获取清洗后的 因子及其 未来收益(可以包含行业，也可以不包含行业)，并将它们的收益对齐
factor_data = get_clean_factor_and_forward_returns(assets[['pct_chg']].shift(2), close)

# Alphalens 有一个特别强大的功能叫 tears 模块，它会生成一张很大的表图，
# 里面是一张张被称之为撕页(tear sheet)的图片，记录所有与回测相关的 结果
# create_full_tear_sheet(factor_data, long_short=False)
long_short = True
group_neutral = False
by_group = False
plotting.plot_quantile_statistics_table(factor_data)
create_returns_tear_sheet(factor_data, long_short, group_neutral, by_group, set_context=False)
create_information_tear_sheet(factor_data, group_neutral, by_group, set_context=False)
create_turnover_tear_sheet(factor_data, set_context=False)

"""
python -m alphalen.test1
"""
