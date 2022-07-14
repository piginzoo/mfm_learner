from mfm_learner.utils.tushare_download.downloaders.base.base_downloader import BaseDownloader
from mfm_learner.utils.tushare_download.downloaders.base.default_downloader import DefaultDownloader

"""
stock_basic
----------------------------
名称       类型 默认显示    描述
ts_code     str Y       TS代码
name        str Y       股票名称
area        str Y       地域
industry    str Y       所属行业
market      str Y       市场类型（主板/创业板/科创板/CDR）
list_status str N       上市状态 L上市 D退市 P暂停上市
list_date   str Y       上市日期
delist_date str N       退市日期

stock_company
----------------------------
名称          类型 默认显示 描述
ts_code     str     Y 股票代码
reg_capital float   Y 注册资本
city        str     Y 所在城市
employees   int     Y 员工人数
"""
import logging
from mfm_learner.utils import utils

logger = logging.getLogger(__name__)


# 查询当前所有正常上市交易的股票列表
# L 是没有退市的

class StockBasic(DefaultDownloader):
    def get_table_name(self):
        return "stock_basic"

    def get_func(self):
        return self.pro.stock_basic

    def get_func_kwargs(self):
        return {'fields': 'ts_code,name,area,industry,market, list_status, list_date, delist_date'}


# python -m mfm_learner.utils.tushare_download.downloaders.stock_basic
if __name__ == '__main__':
    utils.init_logger()
    downloader = StockBasic()
    downloader.download()
