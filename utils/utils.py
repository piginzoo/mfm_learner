import datetime
import logging
import os
import warnings

import yaml
import matplotlib.pyplot as plt
from backtrader.plot import Plot_OldSync
from pandas import Series

import conf
import utils

logger = logging.getLogger(__name__)

from sqlalchemy import create_engine

DB_FILE = "../data/tushare.db"


def load_config():
    if not os.path.exists(conf.CONF_PATH):
        raise ValueError("配置文件[conf/config.yml]不存在!(参考conf/config.sample.yml):" + conf.CONF_PATH)
    f = open(conf.CONF_PATH, 'r', encoding='utf-8')
    result = f.read()
    # 转换成字典读出来
    data = yaml.load(result, Loader=yaml.FullLoader)
    logger.info("读取配置文件:%r", conf.CONF_PATH)
    return data


def connect_db():
    uid = utils.CONF['datasources']['mysql']['uid']
    pwd = utils.CONF['datasources']['mysql']['pwd']
    db = utils.CONF['datasources']['mysql']['db']
    host = utils.CONF['datasources']['mysql']['host']
    port = utils.CONF['datasources']['mysql']['port']
    engine = create_engine("mysql+pymysql://{}:{}@{}:{}/{}?charset={}".format(uid, pwd, host, port, db, 'utf8'))
    # engine = create_engine('sqlite:///' + DB_FILE + '?check_same_thread=False', echo=echo)  # 是否显示SQL：, echo=True)
    return engine


def str2date(s_date, format="%Y%m%d"):
    return datetime.datetime.strptime(s_date, format)


def date2str(date, format="%Y%m%d"):
    return datetime.datetime.strftime(date, format)

def dataframe2series(df):
    if type(df) == Series: return df
    assert len(df.columns)==1, df.columns
    return df.iloc[:,0]


def init_logger():
    logging.basicConfig(format='%(asctime)s:%(filename)s:%(lineno)d:P%(process)d:%(levelname)s : %(message)s',
                        level=logging.DEBUG,
                        handlers=[logging.StreamHandler()])

    logging.getLogger("requests").setLevel(logging.WARNING)
    logging.getLogger("urllib3").setLevel(logging.WARNING)
    logging.getLogger('matplotlib.font_manager').disabled = True
    logging.getLogger('matplotlib').disabled = True
    logging.getLogger('fontTools.ttLib.ttFont').disabled = True
    warnings.filterwarnings("ignore")
    warnings.filterwarnings("ignore", module="matplotlib")
    logger = logging.getLogger()
    logger.setLevel(logging.DEBUG)

class MyPlot(Plot_OldSync):
    def show(self):
        plt.savefig("debug/backtrader回测.jpg")

