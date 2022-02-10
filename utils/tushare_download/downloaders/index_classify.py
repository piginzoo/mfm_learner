import logging

from utils import utils
from utils.tushare_download.downloaders.base_downloader import BaseDownload

logger = logging.getLogger(__name__)


class IndexClassify(BaseDownload):

    def get_table_name(self):
        return "index_classify"

    def download(self):
        df = self.retry_call(func=self.pro.index_classify)
        logger.debug("下载了行业分类index_classify %d 条",len(df))
        self.to_db(df)


# python -m utils.tushare_download.downloaders.index_classify
if __name__ == '__main__':
    utils.init_logger()
    downloader = IndexClassify()
    downloader.download()
