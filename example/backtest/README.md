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