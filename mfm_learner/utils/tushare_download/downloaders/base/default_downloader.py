from mfm_learner.utils.tushare_download.downloaders.base.base_downloader import BaseDownloader
import logging

logger = logging.getLogger(__name__)


class DefaultDownloader(BaseDownloader):
    """
    最简单的默认下载器
    """

    def download(self):
        df = self.retry_call(func=self.get_func(),**self.get_func_kwargs())
        logger.debug("下载了[%s]数据 %d 条", self.get_table_name(), len(df))
        self.to_db(df, if_exists="replace")
