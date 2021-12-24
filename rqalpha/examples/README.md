rqalpha是米筐开源的一个回测项目，类似于backtrader，对标的话。

# 下载数据

它有个酷的特性，就是把数据当到本地运行：

```
rqalpha download-bundle -d <target_bundle_path>
```
如果不指定-d，默认是下载都`~/.rqalpha`下，合计1.5G数据：
```
>ll ~/.rqalpha/bundle
total 3104416
-rw-rw-r--  1 piginzoo  staff   4.6M 12  1 19:00 dividends.h5
-rw-rw-r--  1 piginzoo  staff   2.6M 12  1 19:00 ex_cum_factor.h5
-rw-rw-r--  1 piginzoo  staff   104M 12  1 19:00 funds.h5
-rw-rw-r--  1 piginzoo  staff   730K 12  1 19:01 future_info.json
-rw-rw-r--  1 piginzoo  staff   175M 12  1 19:00 futures.h5
-rw-rw-r--  1 piginzoo  staff   475M 12  1 19:00 indexes.h5
-rw-rw-r--  1 piginzoo  staff    21M 12  1 19:00 instruments.pk
-rw-rw-r--  1 piginzoo  staff   1.7K 12  1 19:00 share_transformation.json
-rw-rw-r--  1 piginzoo  staff   1.7M 12  1 19:00 split_factor.h5
-rw-rw-r--  1 piginzoo  staff   5.6M 12  1 19:00 st_stock_days.h5
-rw-rw-r--  1 piginzoo  staff   719M 12  1 19:01 stocks.h5
-rw-rw-r--  1 piginzoo  staff   5.9M 12  1 19:00 suspended_days.h5
-rw-rw-r--  1 piginzoo  staff    34K 12  1 19:00 trading_dates.npy
-rw-rw-r--  1 piginzoo  staff   730K 12  1 19:00 yield_curve.h5
```

# 文档
- [rqalpha的api文档](https://rqalpha.readthedocs.io/zh_CN/latest/intro/overview.html)

# 运行例子

```
rqalpha run -s 2014-01-01 -e 2016-01-01 \
-f rqalpha/examples/golden_cross.py \
--account stock 100000 -p -bm 000001.XSHE
```
