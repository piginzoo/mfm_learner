这个是我多因子学习的沙盒，各种尝试，学习和练习，都放到里面。

# 开发日志
- 2021.12.22 example/ 实现一个[完整的例子](example/README.md)
- 2021.12.20 btrader/，backtrader学习实践
- 2021.12.18 fama/，尝试复现fama三因子模型
- 2021.12.17 讲LNCAP因子使用alphales做了一遍，market_value_factor_alpha_lens.py
- 2021.12.16 实现了一个新的因子，LNCAP市值因子的验证
- 2021.12.14 实现了CLV因子的所有的验证

# 依赖
alphalens有些小bugs，fork之并修复，所以项目中使用的我自己的[alphalens](https://github.com/piginzoo/alphalens)：

`pip install git+https://github.com/piginzoo/alphalens.git`

# 参考

## 数据源
- [tushare的api文档](https://tushare.pro/document/2?doc_id=95)
- [rqalpha的api文档](https://rqalpha.readthedocs.io/zh_CN/latest/intro/overview.html)
- [jqdata的api文档](https://www.joinquant.com/help/api/help#name:Stock)

## 代码
- [很赞的一个资源集合项目awesome-quant](https://github.com/thuquant/awesome-quant)
- https://github.com/zhangjinzhi/Wind_Python.git
- https://github.com/a1137261060/Multi-Factor-Models.git
- https://github.com/JoshuaQYH/TIDIBEI
- https://github.com/JoinQuant/jqfactor_analyzer.git
- https://github.com/Spark3122/multifactor.git

# 吐槽
- [聚宽的JQdata](https://www.joinquant.com/data)免费用，所有的数据都可以用，但是只能半年，半年后续费，3000元+/年，小贵，不过可以获得他的因子库，未来考虑吧
- [tushare](https://tushare.pro/)也是免费，但是只限于少量api接口，大部分有用的必须充值会员，200元档基本够用，500元就是vip无限制了，1000元可以入高级群，呵呵
- 不考虑在线服务了，太难用。尝试了[优矿](https://uqer.datayes.com/labs/)，很难用；估计[米筐](ricequant.com)、[聚宽](https://www.joinquant.com/)的在线版也好不了哪去；且，这几家都不再搞个人宽客方向了，都给机构服务去了；米筐连社区都关了。

# 其他
rqalpha，是rqalpha的练习代码，源自[rqalpha中的例子](https://rqalpha.readthedocs.io/zh_CN/latest/intro/tutorial.html)。