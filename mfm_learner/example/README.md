# 一个完整的多因子量化项目

通过一个例子，串起数据、因子筛选&合成、回测等诸多细节，成为下一步更实战的量化投资做准备。

## 为了学习和验证

- 规模因子、动量因子、价值因子、盈利因子
- 使用全A的股票，2008~2021，目前使用中证500，后续使用全量，500x13x365=2372500，250万条数据
- 跑出4个因子后，对比看看哪一个好，用alphalens做比较
- 然后用这4个因子，合成一个因子，用IC法来合成
- 用backtrader跑回测，看最优的参数，学习使用Analyser
- 学习人家如何使用遗传算法、粒子算法做优化

## 这个项目的意义

- 对主流因子有一个了解
- 学会数据的清洗细节，更进一步掌握pandas
- 学会整套的因子处理流程，加深对细节的理解
- 熟悉alphalens和BackTrader两大框架，并形成自己的脚手架代码

## 如何运行

### 1.安装依赖

运行：

`source bin/env.sh`

本来想做个docker，做到一半没时间继续搞了，还是virtualenv大法简单一些。

### 2.准备好配置

准备好`conf/config.yml`，可以参考`conf/config.sample.yml`文件编写此文件。

默认为支持的数据源为tushuare，所以，请提前自行注册[tushare](https://tushare.pro)账号:

```yaml
datasource: 'tushare' 
```

### 3. 运行程序

运行：

- 单因子测试：`python -m mfm_learner.example.factor_analyzer`
- 多因子测试：`python -m mfm_learner.example.factor_synthesizer`
- 因子选股回测：`python -m mfm_learner.example.factor_backtester`

目前是测试的2014~2019年5年的数据，股票池使用的是中证500。

### 使用数据库

`python -m mfm_learner.utils.db_creator`

我们使用的数据是 [B站UP主致敬大神](https://www.bilibili.com/video/BV1564y1b7PR) 提供的离线tushare数据（数据是有偿的）。
通过这个命令，可以把csv导入到本地的mysql，并可以创建索引， 这样做在加载单只股票的时候可以加速。

如果不想这么麻烦，可以直接调用tushare的数据API，只不过有时候会限流。

## 研发中的思考

- 究竟格式是 date,stock,factor，还是，date，stock1，stock2，....，这种格式呢？ 其实不用纠结，alphalens就是前者，而，jaqs就是后者，其实无所谓，我自己觉得，定死一种就可以。
  然后使用stack和unstack就可以转换，[参考](../test/test_index.py)。
- 在计算IC的时候，你的换仓周期很重要，你必须要用你的因子值，去和下个选股周期的股票收益率做相关性分析， 比如你是10日换仓，那你就要用前10日的平均PB，去预测第11天的收益率，而且，这个收益率肯定是一个累计收益率，
  也就是从这个10日的第一天，到11天之间的10天累积的收益率。 这就要求，你在做IC、IR计算的时候，调仓周期就是个超参了，所以，这个是可以作为未来优化的参数了。
- 完整描述一下我认为的因子选股的过程：
    ```
    先做单因子筛选：确定持仓周期后，看这个因子和下期收益的相关性，即IC，也可以算出IR和RankIC。
    通过所有天（还是持仓周期？）的数据做回归，回归出每天的每个因子的收益率（不过计算出每个因子的收益率有什么用？）
    难道用收益率的t检验，来看这个因子靠谱么？
    因子靠谱，就应该因子的收益率都不为0才可以，所以假设检验是因子收益率都≠0？
    得到的因子暴露值，还需要做行业、市值中性化，一般都只做这2个。
    这样就得到一个可用的，纯净的因子。
    然后得到一个因子库，
    然后用这个因子库，彼此之间做施密特正交，我理解是，正交后，还是得到跟原来一样的维度和数量的因子们。
    然后再按照IC相关性、等权重、IR等，分别作为权重，去合成这些因子成为一个因子，
    做这步，还是为了简化（非要合成成一个因子么？到底有什么好处？）
    然后，拿这个新的因子去做一个分层回测，看看是不是真的发散，视觉上感知他的效果。
    然后，用这个因子，去预测，也就是每次用它去给股票池里的股票打分，
    什么叫打分？就是用它去算一个因子暴露（注意，不是因子收益），
    找出那些暴露大的，如果是一个负向因子，就是找出暴露小的，得到新的备选股票，用他们去调仓。
    ```
- z-score标准化，标准化的是谁？其实应该是将每个因子自己"自查"，比如我是市值因子，都几个亿啥的；你是动量因子，都是零点积； 两个完全没法比啊？咋办？那就都各自先回到\[0,1\]之间，这样大家都在一个标准之上了。
- 同理，去极值（MAD）、用中位数（均值也可以）填充NAN，也是用因子自己的值，也就是df.group(factor_name).apply(xxx)
- 单个因子搞完了，要做去极值、填充NAN、和标准化
- 合成因子后，还需要再做一遍
- 关于回测

  我使用的是backtrader做的回测，backtrader可能是大家用的最多的回测框架，但是我的多因子回测比较特殊，还是有不少坑。
  [参考1](https://zhuanlan.zhihu.com/p/351751730),[参考2](https://www.bilibili.com/video/BV1VU4y1W7KN)
    - 核心是数据，我需要灌入多个股票的数据，其中第一个是中证500的指数，然后才是其他50只股票，为何要第一个是中证500呢，
      原因是为了对齐，因为50只股票里今天你停牌，明天我停牌，最好的就是用他们的股票池，也就是中证500，做日期的对齐
    - 每支股票其实对应到backtrader就是一个line，所以我总共有51个lines
    - 另外一个就是数据的格式，backtrader要求类必须是: `datetime, open, high, low, high, close, volume, openinterest`
      每个column的顺序都有顺序，但是datetime没有，你要么指定datetime的顺序，要么直接把他设成index，否则，会报错
    - 我在策略列中，实现了多因子选股的逻辑：
        ```    
        我自己的多因子策略，即，用我的多因子来进行选股，股票池是中证500，每一个选股周期，我都要根据当期数据，去计算因子暴露（因子值），
        然后根据因子值，对当前期中证500股票池中的股票进行排序，（这个期间中证500可能备选股票可能会变化）
        然后，选择前100名进行投资，对比新旧100名中，卖出未在list中，买入list中的，如此进行3年、5年投资，看回测收益率。
        ```
    - 我需要获得当前日期，datas[0]就是当天, 但是之前用`self.datas[0].date(0)`
      取不行，因为df.index是datetime类型的，需要改成`self.datas[0].datetime.datetime(0)`
    - 买入、清仓函数，必须要传入到当前的股票，而当前股票的引用是通过名字查找到的，而名字是在最开始数据feed时候绑定的
        ```
            stock_data = self.getdatabyname(sell_stock) # 根据名字获得对应那只股票的数据
            self.close(data=stock_data, exectype=bt.Order.Limit)
            self.buy(data=stock_data, size=size, price=open_price, exectype=bt.Order.Limit)
        ```

## 参考

### 因子分析的开源项目

- [Quantopian虽然倒闭了，但开源出来因子分析工具造福天下，大家都是抄他的](https://github.com/piginzoo/alphalens)
- [JAQS,量化开源会研发的一套策略分析工具](https://github.com/piginzoo/JAQS)
- [jqfactor_analyzer，聚宽开源出来的一套因子分析工具](https://github.com/piginzoo/jqfactor_analyzer)，它只有单因子分析。
- [jaqs-fxdayu，新格投资做的JAQS扩展](https://github.com/xingetouzi/jaqs-fxdayu)

### 一些参考项目

- https://github.com/Weiwei-Ch/AI-trading-alpha.git
- https://github.com/Quant132/BackTrader_Multifactors.git
- https://github.com/SAmmer0/GeneralLib.git
- https://github.com/hugo2046/GetAstockFactors.git
- https://github.com/LHospitalLKY/JointQuant_Learning.git
- https://github.com/a1137261060/Multi-Factor-Models.git
- https://github.com/ChannelCMT/OFO.git
- https://github.com/ChannelCMT/QS.git
- https://github.com/ChannelCMT/QTC_2.0.git
- https://github.com/Miya-Su/Quantitative-Trading.git
- https://github.com/hugo2046/Quantitative-analysis.git
- https://github.com/JoshuaQYH/TIDIBEI
- https://github.com/zhangjinzhi/Wind_Python.git
- https://github.com/lc-sysbs/alpha101.git
- https://github.com/lc-sysbs/alpha101-new.git
- https://github.com/phonegapX/alphasickle.git
- https://github.com/iLampard/alphaware.git
- https://github.com/jcchao/deeplearning-for-quant.git
- https://github.com/Hatonnlatwiira/factor-model-tushare.git
- https://github.com/jiangtiantu/factorhub.git
- https://github.com/Jensenberg/multi-factor.git
- https://github.com/Spark3122/multifactor.git
- https://github.com/ricequant/rqalpha.git

### 参考书籍

- [《股票多因子模型实战：Python核心代码解析》](https://book.douban.com/subject/35446933/)，这本书深入浅出，对新手很友好
- [《因子投资：方法与实践》](https://book.douban.com/subject/35192979/)，石川大佬的书，经典的和，很多细节讨论很深入

## 因子库

- [优矿提供的因子库列表](https://uqer.datayes.com/data/search/MktStockFactorsOneDayGet)
- [聚宽提供的因子列表](https://www.joinquant.com/help/api/help#name:factor_values)
- [万矿社区策略和研究汇总](https://www.windquant.com/qntcloud/article?3d622808-4060-41f1-ad69-6abc42a246bc)
- [各平台开源的选股策略汇总](https://www.jianshu.com/p/e025812bba01)
- [BigQuant多因子汇总](https://bigquant.com/wiki/doc/yinzi-P6fkGbfao2)
- [动量因子](https://zhuanlan.zhihu.com/p/379269953)
- [动量类因子](https://www.windquant.com/qntcloud/article?3aef2d11-8fab-427b-a6b5-ef50cdbe997b)

