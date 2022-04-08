import logging

from utils import utils
from utils.tushare_download.downloaders.base_downloader import BaseDownload
from utils.tushare_download.downloaders.periodly_downloader import PeriodlyDownloader

logger = logging.getLogger(__name__)

fields = ['index_code','industry_name','parent_code','level','industry_code','is_pub','src']

class IndexClassify(BaseDownload):
    """
    下载行业分类信息
    """

    def get_table_name(self):
        return "index_classify"

    def download(self):
        df = self.retry_call(func=self.pro.index_classify, fields=fields)
        logger.debug("下载了行业分类index_classify %d 条",len(df))
        self.to_db(df,if_exists="replace")


# python -m utils.tushare_download.downloaders.index_classify
if __name__ == '__main__':
    utils.init_logger()
    downloader = IndexClassify()
    downloader.download()
