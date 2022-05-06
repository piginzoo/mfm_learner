from utils.tushare_download.downloaders.base.base_downloader import BaseDownloader

"""
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


class StockCompany(BaseDownloader):

    def download(self):
        df_stock_company = self.pro.stock_company(exchange='', fields='ts_code,reg_capital,city,employees')

        logger.debug("下载公司信息 [%d]条", len(df_stock_company))

        # 数据量不大，直接全部重新下载，replace数据库中的数据
        self.to_db(df_stock_company, if_exists='replace')

    def get_table_name(self):
        return "stock_company"


# python -m utils.tushare_download.downloaders.stock_company
if __name__ == '__main__':
    utils.init_logger()
    downloader = StockCompany()
    downloader.download()
