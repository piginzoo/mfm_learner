import logging
import time
from multiprocessing import Process
import pandas as pd
logger = logging.getLogger(__name__)


# 把任务平分到每个work进程中，多余的放到最后一个进程，10=>[3,3,4], 11=>[4,4,3]，原则是尽量均衡
def split(_list, n):
    k, m = divmod(len(_list), n)
    return (_list[i * k + min(i, m):(i + 1) * k + min(i + 1, m)] for i in range(n))


def execute(data, worker_num, function, **params):
    """
    多进程处理器
    :param data_list: 数据列表，可以是list，也可以是dict（dict会被转成list[0,1])
    :param worker_num: 启动多少个进程处理
    :param function: 执行函数，这个函数的参数有要求，一定是 (数据：函数可以自己来约定, id, queue）
    :return:
    """
    start = time.time()
    producers = []

    # convert to list
    if type(data) == dict:
        data = [[k, v] for k, v in data.items()]

    if type(data)==pd.Series: data = data.tolist()
    assert type(data) == list, "invalid data type,must be list, you are:" + str(type(data))

    split_data_list = split(data, worker_num)

    for id, worker_data_list in enumerate(split_data_list):
        params['stocks'] = worker_data_list
        logger.debug("进程[#%d]参数：%r",id, params)
        p = Process(target=function,kwargs=params)
        producers.append(p)
        p.start()
    for p in producers:
        p.join()  # 要等着这些子进程结束后，主进程在往下

    all_seconds = time.time() - start
    minutes = all_seconds // 60
    seconds = all_seconds % 60

    logger.info("%d个工作进程运行完毕，处理[%d]条数据，耗时: %d 分 %d 秒",
                id+1,
                len(data),
                minutes,
                seconds)
