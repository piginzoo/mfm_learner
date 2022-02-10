import logging
import time

from . import utils

CONF = utils.load_config()

logger = logging.getLogger("函数耗时")


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
