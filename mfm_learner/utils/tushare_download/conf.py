from mfm_learner.utils import db_utils
MAX_RETRY = 7  # 尝试几次：5,10,20,40,80,160,320,640
INTERVAL_STEP = 5  # 2次API调用间的间隔，异常后会倍数递增：https://tushare.pro/document/1?doc_id=290 ,每分钟可以访问多少次，200元/年的账号默认是400/分钟，但是大量访问会降级到200/分钟，所以可能要经常手工调整，为了提取，设置成300次/分钟
SLEEP_INTERVAL = 30 # 下载发生异常等待retry的时间
EALIEST_DATE = db_utils.EALIEST_DATE # 最早的数据起始年份，默认是20080101，股改后
MAX_STOCKS_BATCH = 200 # 对于支持批次的API，做多一个批次提交的股票数
TODAY_TIMING = 16 # 今天的数据产生的时间点