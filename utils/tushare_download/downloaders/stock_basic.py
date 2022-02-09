import tushare as ts

from utils.tushare_download.downloaders.base_downloader import BaseDownload

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
from utils import utils

logger = logging.getLogger(__name__)


# 查询当前所有正常上市交易的股票列表
# L 是没有退市的

class StockBasic(BaseDownload):

    def download(self):

        df_stock_basic = self.pro.stock_basic(exchange='',
                                         list_status='L',
                                         fields='ts_code,name,area,industry,market, list_status, list_date, delist_date')

        logger.debug("下载股票基本信息 [%d]条", len(df_stock_basic))

        # 数据量不大，直接全部重新下载，replace数据库中的数据
        self.to_db(df_stock_basic, "stock_basic", if_exists='replace')


# python -m utils.tushare_download.stock_basic
if __name__ == '__main__':
    utils.init_logger()
    downloader = StockBasic()
    downloader.download()
