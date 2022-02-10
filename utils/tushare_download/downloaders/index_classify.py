import logging

from utils import utils
from utils.tushare_download.downloaders.base_downloader import BaseDownload

logger = logging.getLogger(__name__)


class IndexClassify(BaseDownload):

    def get_table_name(self):
        return "index_classify"

    def download(self):
        df = self.retry_call(func=self.pro.index_classify)
        logger.debug("下载了行业分类 [%s] %s~%s 的index_weight %d 条",
                     self.index_code,
                     self.start_date,
                     self.end_date,
                     len(df))
        self.to_db(df)


# python -m utils.tushare_download.downloaders.index_classify
if __name__ == '__main__':
    utils.init_logger()
    downloader = IndexClassify()
    downloader.download()
