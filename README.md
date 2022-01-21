这个是我多因子学习的沙盒，各种尝试，学习和练习，都放到里面。

# 开发日志
- 2022.1.21 实现了按照多个因子单独评判得分的总分来选股（之前是因子合成策略），并对代码进行了重构
- 2022.1.18 实现了中性化(市值和行业)处理，处理了tushare行业代码的问题
- 2022.1.13 回去理解alphalens的各种分析结果，并依据它形成一个自己的因子评分 factor_combiner.py::score()
- 2022.1.6 终于跑通了单因子检验+多因子合成+合成因子选股+backtrader回测，happy~ :)
- 2022.1.3 utils/db_creater，将["致敬大神"](https://www.bilibili.com/video/BV1564y1b7PR?p=5)的csv数据导入到数据库
- 2022.1.3 example/testback.py，使用backtrader做回测
- 2021.12.22 example/ 实现一个[完整的例子](example/README.md)
- 2021.12.20 btrader/，backtrader学习实践
- 2021.12.18 fama/，尝试复现fama三因子模型
- 2021.12.17 讲LNCAP因子使用alphales做了一遍，market_value_factor_alpha_lens.py
- 2021.12.16 实现了一个新的因子，LNCAP市值因子的验证
- 2021.12.14 实现了CLV因子的所有的验证

# 项目

这里涉及到一些里的项目，不过主要项目为[/example](example/README.md)下的例子项目。

# 参考

## 常用API
- [rqalpha的api文档](https://rqalpha.readthedocs.io/zh_CN/latest/intro/overview.html)
- [alphalens](http://pg.jrj.com.cn/acc/Res/CN_RES/INVEST/2017/12/6/26dee515-a988-4259-915d-e40c6ab28e45.pdf)
- [backtrader](http://backtrader.com.cn/docu/#4)

## 数据源
- [tushare的api文档](https://tushare.pro/document/2?doc_id=95)
- [jqdata的api文档](https://www.joinquant.com/help/api/help#name:Stock)
- [B站'直径大神'的UP主整理tushare数据的教程](https://www.bilibili.com/video/BV1564y1b7PR)

## 代码
- [很赞的一个资源集合项目awesome-quant](https://github.com/thuquant/awesome-quant)
- https://github.com/zhangjinzhi/Wind_Python.git
- https://github.com/a1137261060/Multi-Factor-Models.git
- https://github.com/JoshuaQYH/TIDIBEI
- https://github.com/JoinQuant/jqfactor_analyzer.git
- https://github.com/Spark3122/multifactor.git
- https://github.com/ChannelCMT/OFO

### alpha101
- [WorldQuant 101 Formulaic Alphas paper](https://arxiv.org/pdf/1601.00991.pdf)
- [WorldQuant 101 Alpha、国泰君安 191 Alpha](https://mp.weixin.qq.com/s?__biz=MzAxNTc0Mjg0Mg==&mid=2653290927&idx=1&sn=ecca60811da74967f33a00329a1fe66a)
- https://github.com/STHSF/alpha101
- https://github.com/laox1ao/Alpha101-WorldQuant
- https://github.com/yutiansut/QAFactor_Alpha101
- https://github.com/lc-sysbs/alpha101


# 吐槽
- [聚宽的JQdata](https://www.joinquant.com/data)免费用，所有的数据都可以用，但是只能半年，半年后续费，3000元+/年，小贵，不过可以获得他的因子库，未来考虑吧
- [tushare](https://tushare.pro/)也是免费，但是只限于少量api接口，大部分有用的必须充值会员，200元档基本够用，500元就是vip无限制了，1000元可以入高级群，呵呵
- 不考虑在线服务了，太难用。尝试了[优矿](https://uqer.datayes.com/labs/)，很难用；估计[米筐](ricequant.com)、[聚宽](https://www.joinquant.com/)的在线版也好不了哪去；且，这几家都不再搞个人宽客方向了，都给机构服务去了；米筐连社区都关了。

# 其他
rqalpha，是rqalpha的练习代码，源自[rqalpha中的例子](https://rqalpha.readthedocs.io/zh_CN/latest/intro/tutorial.html)。