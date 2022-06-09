ptstrategy(strategy, *args, **kwargs)
Adds a Strategy class to the mix for optimization. Instantiation will happen during run time.
args and kwargs MUST BE iterables which hold the values to check.

靠，无法传入因子数据了，
我现在有2个选择：
1、在数据中并入因子数据，即在最后加入因子的列。
2、把因子传入，还要传入start,end,stocks，当然，可以从line里面去读取
我更倾向于第一种，毕竟2在strategy肚子里，干的也是这事。
但是，这个前提是，所有的因子一定是stock+date的格式的。
另外，如果是多个因子，那就需要放到数据中，加入多个因子列，
如果是合成因子，那么就把合成因子当做单因子对待。
但是，如果是FF3呢？他其实是就是每天的因子收益率，没有股票的对应。
这种，我想，直接当做一只股票对待吧，放在第一个或者最后一个之类。
恩，我不应该放弃非优化方式optstrategy的方式，它更灵活。

# Line的概念和多股票

backtrader的lines对象：

在策略类中，获得的self.datas，包含了多只股票的，所以要用self.datas[0]才能应用到第一只股票的。
一直股票的又是个多line的结构，即self.datas[0].lines是一只股票的数据的所有列，每一列就是一个lines。
但是backtrader有各种缩写，你可以直接缩写成self.close，就是，self.datas[0].lines.close。

# 设计上的纠结

本来最开始是，把因子数据，df_factor作为构造函数传入策略类，
在试用cerebo.addstrategy()的时候，没问题，

后来我想用cerebo.optstrategy()的时候，就不支持传入构造函数了，
在optstrategy的时候，他只支持传入params，而不支持之前的addstrategy支持构造函数参数了，
但是，你用params传入，他又会自动product()，读了源码理解的，就是自动展开范围，
结果导致的是，他会自动展开dataframe，所以我尝试传入一个大df_factor的企图失败了，
吐槽一下，这个bt的设计不一致性让我感觉不爽，
然后，我看了bt论坛上使用动态类
```python
def create_data_feed_class(factor_names):
    """
    需要用动态创建类的方式，来动态扩展bakctrader的PandasData，
    原因是他的这个类狠诡异，要么就在类定义的时候就定义params，
    否则，你不太可能在构造函数啥的里面去动态扩展params，
    但是我们不同的factors可能数量不同，为了要合并到PandasData的列中，
    就需要这种动态方式，参考的是backtrader上例子。
    """

    lines = tuple(factor_names)
    params = tuple([(factor_name, -1) for factor_name in factor_names])
    return type('PandasDataFeed', (PandasData,), {'lines': lines, 'params': params})

PandasDataClass = create_data_feed_class(factor_names)
data = PandasDataClass(dataname=df_stock, fromdate=d_start_date, todate=d_end_date, plot=False)
```
这个方案最开始，把因子的列，贴在股票数据的后面的列上，然后给起个df_xxx的列名以示区别，
因为因子数是不定的，所以需要在定义PandasData的时候，动态增加列名，用了动态类方式。
但是，这个方案也有问题，
就是在策略类的next迭代的时候，我还要找出因子列们，通过他们的排序，来确定我选择哪些股票，
但是，bt的lines机制，基本上是隔离各个股票数据的，
我自己感觉，对界面支持不好，我为了比较，我需要一个包含了股票和因子们值的数据，
```text
    code        factor_clv        factor_mv                  ...
    300433.SZ   -5.551115e-17     -5.551115e-17              ...
    300498.SZ    0.000000e+00     -5.551115e-17              ...
    ...         ...               ...                        ...
```
我还需要从lines里面重新组装这个值，我靠，太费劲了。

所以，最终我采用了，通过传入因子名称的方法，在策略类的__init__中，加载因子数据的方法了，
因子名字还得用逗号分隔，否则，丫又给product了。

# 胜率计算

# 换手率

# 交易统计
