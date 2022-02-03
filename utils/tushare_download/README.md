参考使用致敬大神的代码，
他是下载到一个文件里，
然后我写了一个insert到数据库中的代码，
现在要合并到一起了。

合并逻辑，是找到日期最大的一天，

`select max(trade_date) from daily_hfq;`

然后，去启动每一个表，去tushare download最新的数据，
为了防止失败，先下载成一个文件，然后再从文件导入，
