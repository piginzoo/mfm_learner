import conf
import yaml
import logging
import os
logger = logging.getLogger(__name__)


def load_config():
    if not os.path.exists(conf.CONF_PATH):
        raise ValueError("指定的环境配置文件不存在:" + conf.CONF_PATH)
    f = open(conf.CONF_PATH, 'r', encoding='utf-8')
    result = f.read()
    # 转换成字典读出来
    data = yaml.load(result, Loader=yaml.FullLoader)
    logger.info("读取配置文件:%r", conf.CONF_PATH)
    return data