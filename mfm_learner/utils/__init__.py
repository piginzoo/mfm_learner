import logging
import os
import time

import yaml

import conf

logger = logging.getLogger("函数耗时")


def load_config():
    if not os.path.exists(conf.CONF_PATH):
        raise ValueError("配置文件[conf/config.yml]不存在!(参考conf/config.sample.yml):" + conf.CONF_PATH)
    f = open(conf.CONF_PATH, 'r', encoding='utf-8')
    result = f.read()
    # 转换成字典读出来
    data = yaml.load(result, Loader=yaml.FullLoader)
    logger.info("读取配置文件:%r", conf.CONF_PATH)
    return data


CONF = load_config()


def logging_time(func):
    """
    一个包装器，用于记录函数耗时
    """

    def wrapper_it(*args, **kw):
        start_time = time.time()
        result = func(*args, **kw)
        logger.debug("耗时: %.2f 秒", time.time() - start_time)
        return result

    return wrapper_it
