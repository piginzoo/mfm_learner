import logging
import time

import pandas as pd
from pandas.api.types import is_datetime64_any_dtype as is_datetime
from tqdm import tqdm

from conf import DATE_COLUMNS
from datasource import datasource_factory as ds_factory
from utils import CONF, utils

logger = logging.getLogger(__name__)


def reset_index(df, date_only=False, date_format=None):
    """
    把索引设置成[日期+股票代码]的复合索引
    """

    if date_format is None: date_format = CONF['dateformat']

    assert 'datetime' in df.columns, df.columns
    if date_only:
        # 如果是日期类型了，无需再转了
        if not is_datetime(df['datetime']):
            df['datetime'] = to_datetime(df['datetime'], date_format)
        df = df.set_index('datetime')
    else:
        assert 'code' in df.columns, df.columns
        df['datetime'] = to_datetime(df['datetime'], date_format)
        df = df.set_index(['datetime', 'code'])
    return df


def to_datetime(series, date_format=None):
    if date_format is None: date_format = CONF['dateformat']
    return pd.to_datetime(series, format=date_format)  # 时间为日期格式，tushare是str


def date2str(df, date_column):
    df[date_column] = df[date_column].dt.strftime(CONF['dateformat'])
    return df


def load_daily_data(datasource, stock_codes, start_date, end_date):
    df_merge = pd.DataFrame()
    # 每支股票
    start_time = time.time()
    pbar = tqdm(total=len(stock_codes))
    for i, stock_code in enumerate(stock_codes):
        # 得到日交易数据
        data = datasource.daily(stock_code=stock_code, start_date=start_date, end_date=end_date)
        if df_merge is None:
            df_merge = data
        else:
            df_merge = df_merge.append(data)
        pbar.update(i)
    pbar.close()

    logger.debug("一共加载 %s~%s %d 只股票，共计 %d 条日交易数据，耗时 %.2f 秒",
                 start_date,
                 end_date,
                 len(stock_codes),
                 len(df_merge),
                 time.time() - start_time)
    return df_merge


def compile_industry(series_industry):
    """
    把行业列（文字）转换成统一的行业码
    如 "家用电器" => '330000'
    ----------
    index_classify结果：
    index_code  industry_name    level  industry_code is_pub parent_code
    801024.SI          采掘服务    L2        210400   None      210000
    801035.SI          石油化工    L2        220100   None      220000
    801033.SI          化学原料    L2        220200   None      220000
    靠，tushare返回的是中文字符串，不是，申万的行业代码，所以，我得自己处理了。
    ----------
    df_industry: 股票对应的行业中文名，3列[code, date, industry]
    """
    df_industries = ds_factory.get().index_classify()

    def find_industry_code(chinese_name):

        # import pdb;pdb.set_trace()
        # 找列[industry_name]中的值和中文行业名相等的行
        found_rows = df_industries.loc[df_industries['industry_name'] == chinese_name]

        for _, row in found_rows.iterrows():
            r = __extract_industry_code(row)
            if r: return r

        r = __get_possible(df_industries, chinese_name)
        if r is None:
            ValueError('无法找到 [' + chinese_name + "] 对应的行业代码")
        return r

    def __extract_industry_code(row):
        if row.level == 'L1': return row.industry_code
        if row.level == 'L2': return row.parent_code
        if row.level == 'L3':  # 假设一定能找到
            assert len(df_industries.loc[df_industries['industry_code'] == row.parent_code]) > 0
            return df_industries.loc[df_industries['industry_code'] == row.parent_code].iloc[0].parent_code
        raise None

    def __get_possible(df_industries, chinese_name):
        from Levenshtein import distance

        df_industries['distance'] = df_industries['industry_name'].apply(
            lambda x: distance(x, chinese_name)
        )
        first_row = df_industries.sort_values('distance').iloc[0]
        code = __extract_industry_code(first_row)

        # logger.debug("行业纠错：%s=>%s:%s",chinese_name,first_row['industry_name'],code)

        return code

    # 用中文名列，生成，申万的行业代码列, df_industry['industry']是中文名，转成申万的代码：industry_code
    return series_industry.apply(find_industry_code)


def validate_trade_date(df, date_column=None, start_date=None, end_date=None):
    """
    用于检查DataFrame中，有哪些交易日期是缺失的
    :param start_date:
    :param end_date:
    :param df: 被检查的dataframe
    :param date_columns: dataframe中的日期列
    :return:
    """

    assert len(df) > 0, len(df)

    def __find_date_column(columns):
        for col in columns:
            if col in DATE_COLUMNS:
                return col
        return None

    if not date_column:
        date_column = __find_date_column(df.columns)
        if not date_column:
            raise ValueError("Dataframe中不包含日期字段：" + str(df.columns))

    if not start_date:
        series_sort = df[date_column].sort_values()
        start_date = series_sort.iloc[0]
        end_date = series_sort.iloc[-1]

    df_all_trade_dates = ds_factory.get().trade_cal(start_date, end_date)
    df_miss_trade_dates = df_all_trade_dates[~df_all_trade_dates.isin(df[date_column])]
    logger.debug("%s~%s 数据%d行，应为%d个交易日，缺失%d个交易日，缺失率%.1f%%",
                 start_date,
                 end_date,
                 len(df),
                 len(df_all_trade_dates),
                 len(df_miss_trade_dates),
                 len(df_miss_trade_dates) * 100 / len(df_all_trade_dates))
    return df_miss_trade_dates, len(df_miss_trade_dates) / len(df_all_trade_dates)


# python -m datasource.datasource_utils
if __name__ == '__main__':
    utils.init_logger()
    df = ds_factory.get().daily('300152.SZ', '20180101', '20190101')
    validate_trade_date(df)
