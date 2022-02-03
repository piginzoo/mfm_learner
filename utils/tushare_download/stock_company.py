import pandas as pd
import tushare as ts
from pandas import DataFrame

import utils.utils
from utils.tushare_download.base_downloader import BaseDownload

pro = ts.pro_api()

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
import pandas as pd
from utils import utils

logger = logging.getLogger(__name__)


class StockCompany(BaseDownload):

    def download(self):
        df_stock_company_db = pd.read_sql('select * from stock_company', self.db_engine)

        df_stock_company = pro.stock_company(exchange='', fields='ts_code,reg_capital,city,employees')

        ts_codes = df_stock_company['ts_code']
        ts_codes_db = df_stock_company_db['ts_code']

        # 找出新的股票
        ts_new = ts_codes[~ts_codes.isin(ts_codes_db)]

        logger.debug("tushare[%d]条,数据库[%d]条,新的[%d]条", len(ts_codes), len(ts_codes_db), len(ts_new))

        df_new = df_stock_company[df_stock_company['ts_code'].isin(ts_new)]

        self.to_db(df_new, "stock_company")


# python -m utils.tushare_download.stock_company
if __name__ == '__main__':
    utils.init_logger()
    downloader = StockCompany()
    downloader.download()
