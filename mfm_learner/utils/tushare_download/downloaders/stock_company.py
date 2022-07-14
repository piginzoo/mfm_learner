from mfm_learner.utils.tushare_download.downloaders.base.default_downloader import DefaultDownloader

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


class StockCompany(DefaultDownloader):

    def get_func(self):
        return self.pro.stock_company

    def get_table_name(self):
        return "stock_company"


# python -m mfm_learner.utils.tushare_download.downloaders.stock_company
if __name__ == '__main__':
    utils.init_logger()
    downloader = StockCompany()
    downloader.download()
