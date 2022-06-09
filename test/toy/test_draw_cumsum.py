import logging
import time

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import statsmodels.api as sm
from matplotlib.font_manager import FontProperties
from scipy import stats as st
import matplotlib

matplotlib.rcParams['font.sans-serif'] = ['Arial Unicode MS']  # 指定默认字体
matplotlib.rcParams['axes.unicode_minus'] = False  # 正常显示负号
matplotlib.rcParams['axes.unicode_minus'] = False  # 解决负号'-'显示为方块的问题

from mfm_learner import utils

utils.init_logger()
import data_provider

data_provider = data_provider.get('tushare')

logger = logging.getLogger(__name__)

excess_returns_means = pd.read_csv("data/excess_returns_means.csv")

excess_returns_means.info()
# import pdb;pdb.set_trace()
excess_returns_means_cum = excess_returns_means.iloc[:,1:6].apply(lambda x: (1 + x).cumprod().values - 1)

# df = pd.DataFrame(
# [[0.1,0.1,0.1],
#  [0.2,-0.2,0.2],
#  [0.3,-0.3,0.3]], columns=["A","B","C"])
# print(df)
# print(df.apply(lambda x: (1 + x).cumprod().values - 1))

dates = excess_returns_means['trade_date']

# import pdb;pdb.set_trace()

# 画图
plt.clf()
from matplotlib.font_manager import FontProperties
font = FontProperties()
fig = plt.figure(figsize=(16, 6))
ax1 = fig.add_subplot(111)

# ax1.set_xlim(left=0.5, right=len(dates) + 0.5)
# import matplotlib.dates as mdate
# ax1.set_xlabel("", fontproperties=font, fontsize=16)
# ax1.set_xticks(dates)
# ax1.xaxis.set_major_formatter(mdate.DateFormatter('%Y%m%d'))
# ax1.xaxis.set_major_locator(mdate.MonthLocator())
# plt.xticks(pd.date_range('2021-06-01','2021-11-24',freq='10d'))
# ax1.set_xticklabels(pd.date_range('20210601','20211124',freq='10d'), fontproperties=font, fontsize=5)
# ax1.set_ylim(-0.008, 0.008)
ax1.set_ylabel("return", fontproperties=font, fontsize=16)
ax1.set_yticklabels([str(x * 100) + "0%" for x in ax1.get_yticks()], fontproperties=font, fontsize=14)
ax1.set_title("CLV factor return interest", fontproperties=font, fontsize=16)
ax1.grid()
plt.plot(excess_returns_means_cum.iloc[:,:])
plt.legend(excess_returns_means_cum.columns.to_list())
plt.savefig("debug/累计收益图.jpg")
print(excess_returns_means_cum.columns)