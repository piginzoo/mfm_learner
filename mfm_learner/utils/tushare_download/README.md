# 目标

你有一个200元买的tushare pro的key，就可以一键下载需要用的所有数据，缓存到本地数据库中。

这样，后续调用，都使用本地的数据了，大大节省时间和网络开销。

# 数据列表

不用下载所有的，需要的时候，只需要扩展一个新类和接口出来即可

其实，也可以写一个类，把这些下载都组织成一个一键下载，但觉得没必要，
数据还是自己一个个下载清晰，毕竟要对数据了如指掌，
另外，中途可能会出现各种各样的问题，一个一个的下载还是比较稳妥，毕竟这事很久才会做一次。

这些类，都支持从头下载，和增量下载，实现的逻辑是，
先看有没有表，如果有表，就从表里找出日期字段的最新的日期，
然后从数据库中最旧的日期一直下载到当前日期。
当然，不同表的日期字段名和细节都有差异，都在各个类中实现了，不是很麻烦。

- [daily_basic](downloaders/daily_basic.py)：股票每日基本信息
- [daily](downloaders/daily.py)：股票每日交易信息
- [fina_indicator](downloaders/fina_indicator.py)：股票财务信息
- [index_daily](downloaders/index_daily.py)：指数日交易数据
- [index_weight](downloaders/index_weight.py)：指数权重信息
- [index_classify](downloaders/index_classify.py)：行业信息
- [stock_basic](downloaders/stock_basic.py)：股票的基本信息
- [stock_company](downloaders/stock_company.py)：股票的公司信息
- [trade_cal](downloaders/trade_cal.py)：交易日

# 核心父类

定义了几个有用的父类，可以帮助有效的实现下载

- [base_downloader.py](downloaders/base/base_downloader.py)

实现一个下载基类，用于完成，控制下载速度，和反复尝试，以及保存到数据库中的基础功能

- [batch_downloader](downloaders/base/batch_downloader.py)

使用优化完的参数，来下载股票，一次可以支持1只或多只，由参数multistocks决定。
支持多只的时候，需要使用函数calculate_best_fetch_stock_num，计算到底一次下载几只最优。

# 运行

需要手工创建库，表不用，然后一次性拉取，也可以逐一拉取。时间比较长，请耐心。

可运行比较少的交易日历表，来验证拉取流程是否正常：`python -m mfm_learner.utils.tushare_download.downloaders.trade_cal`

1、创建数据库

```
CREATE DATABASE IF NOT EXISTS tushare DEFAULT CHARSET utf8 COLLATE utf8_general_ci;
```

2、配置 conf/config.yml 文件
```
    tushare:
            token: 'bf542ab154ed6bdc1a3842b71ec8c29fab7f78cbdec4a70a04f0bf5f'
    mysql:
            uid: 'root'
            pwd: '123456'
            db:  'tushare'
            host: '127.0.0.1'
            port: 3306
```

3、运行一次性拉取

比较慢，估计3~4个小时：

```
python -m mfm_learner.utils.tushare_download.updator
```

4、也可以，逐一拉取

```
python -m mfm_learner.utils.tushare_download.downloaders.stock_company
python -m mfm_learner.utils.tushare_download.downloaders.stock_basic
python -m mfm_learner.utils.tushare_download.downloaders.trade_cal
python -m mfm_learner.utils.tushare_download.downloaders.daily
python -m mfm_learner.utils.tushare_download.downloaders.daily_basic
python -m mfm_learner.utils.tushare_download.downloaders.index_daily
python -m mfm_learner.utils.tushare_download.downloaders.index_classify
python -m mfm_learner.utils.tushare_download.downloaders.index_weight
python -m mfm_learner.utils.tushare_download.downloaders.fina_indicator
```

5、下载过程中，会将下载完的数据，保存到文件夹`data/tushare_download`中，如果中途失败，或者插入数据库失败，可以手工从文件导入：

例如：

```
# 导入单个文件
python -m mfm_learner.utils.tushare_download.db_importer \
    -f data/tushare_download/fina_indicator_20080101_20220221.csv \
    -t  fina_indicator
```

# 其他

参考使用致敬大神的代码，
他是下载到一个文件里，
然后我写了一个insert到数据库中的代码，
现在要合并到一起了。

合并逻辑，是找到日期最大的一天，

`select max(trade_date) from daily_hfq;`

然后，去启动每一个表，去tushare download最新的数据，
为了防止失败，先下载成一个文件，然后再从文件导入，

