import pandas as pd

from mfm_learner.utils.tushare_download.downloaders.base.base_downloader import BaseDownloader

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
from mfm_learner.utils import utils

logger = logging.getLogger(__name__)


class StockCompany(BaseDownloader):

    def download(self):
        df_SH = self.pro.stock_company(exchange='SSE') # 上交所
        logger.debug("下载沪市上市公司信息 [%d]条", len(df_SH))

        df_SZ = self.pro.stock_company(exchange='SZSE') # 深交所
        logger.debug("下载深市上市公司信息 [%d]条", len(df_SZ))

        df = pd.concat([df_SH,df_SZ])
        logger.debug("合并下载的所有上市公司信息 [%d]条", len(df))

        # 数据量不大，直接全部重新下载，replace数据库中的数据
        self.to_db(df, if_exists='replace')

    def get_table_name(self):
        return "stock_company"


# python -m utils.tushare_download.downloaders.stock_company
if __name__ == '__main__':
    utils.init_logger()
    downloader = StockCompany()
    downloader.download()
