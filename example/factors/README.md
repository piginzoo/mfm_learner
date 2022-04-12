# 概述

多因子是源于fama-french的多因子模型，决定一个股票的价格的因子，主要分为3类：
- 市场因子：这个市场的影响，比如上证指数是个典型因子
- 行业因子：行业的整体特性，比如行业指数
- 风格因子：是个股的上体现出的共同性，通过个股的特有指标反应出来（个股特有指标其实可以认为是这个因子上的风险暴露）

接下来，主要讨论一下风格因子，因为它最多样化和个股相关。
按照《华泰证券多因子系列1：华泰多因子模型体系初探 20160921》，
主要的风格因子分为十二大类:
- 估值因子(Value Factor)
- 成长因子(Growth Factor)
- 财务质量因子(Financial Quality Factor)
- 杠杆因子(Leverage Factor)
- 规模因子(Size Factor)
- 动量因子(Momentum Factor)
- 波动率因子(Volatility Factor)
- 换手率因子 (Turnover Factor)
- 改进的动量因子(Modified Momentum Factor)
- 分析师情绪因子 (Sentiment Factor)
- 股东因子(Shareholder Factor)和技术因子(Technical Factor)。  

**主要参考：**
- 《华泰证券多因子系列1：华泰多因子模型体系初探》 2016.09.21
- 《王雄 广东金融学院量化大赛讲演第四讲 量化投资实践-因子投资的理论基础与实践流程》2020

# 因子列表

  * [估值因子](#估值因子)
    * [账面市值比BM](#账面市值比BM) 
    * [市盈率相对盈利增长比率PEG因子](#市盈率相对盈利增长比率PEG因子)
    * [盈利收益率EP](#盈利收益率EP)
    * [股息率因子DividendRate](#股息率因子DividendRate)
  * [成长因子](#成长因子)
    * [净资产收益率ROE_TTM](#净资产收益率ROE_TTM)
    * [净资产收益率变动ROE_YOY](#净资产收益率变动ROE_YOY)
    * [资产收益率ROA_TTM](#净资产收益率ROA_TTM)
    * [资产收益率变动ROA_YOY](#净资产收益率变动RO_YOY)
    * [税息折旧及摊销前利润EBITDA](#税息折旧及摊销前利润EBITDA)
  * [资产本结构](#资产本结构)
    * [资产负债率AssetsDebtRate](#资产负债率AssetsDebtRate)
    * [固定资产比例](#资产本结构)
    * [流通市值](#流通市值)
  * [技术因子](#技术因子)
    * [动量因子Momentum](#动量因子Momentum)   
    * [CLV因子](#CLV因子)
    * [换手率因子TurnOver](#换手率因子TurnOver)
    * [特质波动率因子ivff](#特质波动率因子ivff)

# 估值因子

## [账面市值比BM](https://github.com/piginzoo/mfm_learner/tree/main/example/factors/BM.py)
账面市值比(book-to-market)：净资产/市值

[知乎参考](https://www.zhihu.com/question/23906290/answer/123700275)，计算方法：
- 1.账面市值比(BM)=股东权益/公司市值.
- 2.股东权益(净资产)=资产总额-负债总额 (每股净资产x流通股数)
- 3.公司市值=流通股数x每股股价
- 4.账面市值比(BM)=股东权益/公司市值=(每股净资产x流通股数)/(流通股数x每股股价)=每股净资产/每股股价=B/P=市净率的倒数

注：账面市值比BM = 1/市盈率PE （*PE=市价/盈利*）

## [市盈率相对盈利增长比率PEG因子](https://github.com/piginzoo/mfm_learner/tree/main/example/factors/peg.py)
参考：
- https://zhuanlan.zhihu.com/p/29144485
- https://www.joinquant.com/help/api/help#factor_values:%E6%88%90%E9%95%BF%E5%9B%A0%E5%AD%90
- https://www.joinquant.com/view/community/detail/087af3a4e27c600ed855cb0c1d0fdfed
在时间序列上，PEG因子的暴露度相对其他因子较为稳定，在近一年表现出较强的趋势性
市盈率相对盈利增长比率PEG = PE / (归母公司净利润(TTM)增长率 * 100) # 如果 PE 或 增长率为负，则为 nan
对应tushare：netprofit_yoy	float	Y	归属母公司股东的净利润同比增长率(%)

财务数据"归属母公司股东的净利润同比增长率(%)"的取得：https://tushare.pro/document/2?doc_id=79
```
    输入参数
        ts_code	str	Y	TS股票代码,e.g. 600001.SH/000001.SZ
        ann_date	str	N	公告日期
        start_date	str	N	报告期开始日期
        end_date	str	N	报告期结束日期
        period	str	N	报告期(每个季度最后一天的日期,比如20171231表示年报)
    输出参数
        ts_code	str	Y	TS代码
        ann_date	str	Y	公告日期
        end_date	str	Y	报告期
```
这里的逻辑是，某一天，只能按照公告日期来当做"真正"的日期，毕竟，比如8.30号公告6.30号之前的信息，
加入今天是7.1日，所以只能用最近的一次，也就是最后一次8.30号的，
再往前倒，比如6.15号还发布过一次，那6.15号之后到6.30号，之间的，就用6.15好的数据，
所以，报告期end_date其实没啥用，因为他是滞后的，外界是无法提前知道的。
这样处理也简单粗暴，不知道业界是怎么处理的？我感觉应该很普遍的一个问题。
```
            shift(-1)
current     next
            2021.1.1
---------------------
2021.1.1    2021.3.1
2021.3.1    2021.6.30
2021.6.30   2021.9.30
2021.9.30
---------------------
2021.1.1之前的，不应该用2021.1.1去填充，但是，没办法，无法获得再之前的数据，只好用他了
2021.9.30之后的，都用2021.9.30来填充
```
季报应该是累计数，为了可比性，所以应该做一个处理
研报上的指标用的都是TTM
PE-TTM 也称之为 滚动市盈率
TTM英文本意是Trailing Twelve Months，也就是过去12个月，非常好理解
比如当前2017年半年报刚发布完，那么过去12个月的净利润就是：
2017Q2 (2017

## [盈利收益率EP](https://github.com/piginzoo/mfm_learner/tree/main/example/factors/ep.py)
其实，就是1/PE（市盈率）
盈利收益率 EP（Earn/Price） = 盈利/价格

这里，就说说PE，因为EP就是他的倒数：

PE = PRICE / EARNING PER SHARE，指股票的本益比，也称为“利润收益率”。 
本益比是某种股票普通股每股市价与每股盈利的比率。 
所以它也称为股价收益比率或市价盈利比率。

- [基本知识解读 -- PE, PB, ROE，盈利收益率](https://xueqiu.com/4522742712/61623733)

## [股息率因子DividendRate](https://github.com/piginzoo/mfm_learner/tree/main/example/factors/dividend_rate.py)
股息率TTM=近12个月股息总额/当日总市值

[tushare样例数据](https://tushare.pro/document/2?doc_id=32)
    
    - dv_ratio	float	股息率 （%）
    - dv_ttm	float	股息率（TTM）（%）

    --------------------------------------------
         ts_code     trade_date  dv_ratio dv_ttm
    0    600230.SH   20191231    3.4573  1.9361
    1    600230.SH   20191230    3.4573  1.9361
    2    600230.SH   20191227    3.4308  1.9212
    3    600230.SH   20191226    3.3629  1.8832
    4    600230.SH   20191225    3.5537  1.9900
    ..         ...        ...       ...     ...
    482  600230.SH   20180108    0.2692  0.2692
    483  600230.SH   20180105    0.2856  0.2856
    484  600230.SH   20180104    0.2805  0.2805
    485  600230.SH   20180103    0.2897  0.2897
    486  600230.SH   20180102    0.3021  0.3021
    --------------------------------------------
    
诡异之处,dv_ratio是股息率，dv_ttm是股息率TTM，
TTM应该比直接的股息率要高，对吧？
我理解普通股息率应该是从年初到现在的分红/市值，
而TTM还包含了去年的分红呢，理应比普通的股息率要高，
可是，看tushare数据，恰恰是反的，困惑ing...

TODO：目前，考虑还是直接用TTM数据了

# 财务质量因子

## [净资产收益率ROE_TTM](https://github.com/piginzoo/mfm_learner/tree/main/example/factors/roe.py)
[ROE计算(https://baike.baidu.com/item/%E5%87%80%E8%B5%84%E4%BA%A7%E6%94%B6%E7%9B%8A%E7%8E%87)
定义公式：

    1、净资产收益率=净利润*2/（本年期初净资产+本年期末净资产）
    2、杜邦公式（常用）
        - 净资产收益率=销售净利率*资产周转率*杠杆比率
        - 净资产收益率 =净利润 /净资产
        -净资产收益率= （净利润 / 销售收入）*（销售收入/ 总资产）*（总资产/净资产）

ROE是和分期相关的，所以年报中的ROE的分期是不同的，有按年、季度、半年等分的，所以无法统一比较，
解决办法，是都画为TTM，即从当日开始，向前回溯一整年，统一成期间为1整年的ROE_TTM(Trailing Twelve Month)
但是，由于财报的发布滞后，所以计算TTM的时候，需要考虑这种滞后性，后面会详细讨论：

    ------------------------------------------
          ts_code  ann_date  end_date      roe
    0   600000.SH  20211030  20210930   6.3866
    1   600000.SH  20211030  20210930   6.3866
    2   600000.SH  20210828  20210630   4.6233
    3   600000.SH  20210430  20210331   2.8901
    4   600000.SH  20210430  20210331   2.8901
    5   600000.SH  20210327  20201231   9.7856
    6   600000.SH  20201031  20200930   7.9413

ROE是净资产收益率，ann_date是真实发布日，end_date是对应财报上的统计截止日，
所以，指标有滞后性，所以，以ann_date真实发布日为最合适，
为了可比性，要做ttm才可以，即向前滚动12个月，
但是，即使TTM了，还要考虑不同股票的对齐时间，
比如截面时间是4.15日：

    A股票：3.31号发布1季报，他的TTM，就是 roe_1季报 + roe_去年年报 - roe_去年1季报
    B股票：4.10号发布1季报，他的TTM，就是 roe_1季报 + roe_去年年报 - roe_去年1季报
    C股票：还没有发布1季报，但是他在3.31号发布了去年年报，所以，他只能用去年的年报数据了
    D股票：还没有发布1季报，也没发布去年年报，所以，他只能用去年的3季报（去年10.31日最晚发布）

    
因子的值，需要每天都要计算出来一个值，即每天都要有一个ROE_TTM，
所以，每天的ROE_TTM，一定是回溯到第一个可以找到的财报发表日，然后用那个发布日子，计算那之前的TTM，

举个例子，
我计算C股票的4.15日的ROE_TTM，就回溯到他是3.31号发布的财报，里面披露的是去年的年报的ROE
我计算B股票的4.15日的ROE_TTM，就回溯到他是4.10号发布的财报，里面披露的是今年1季报ROE+去年的年报ROE-去年1季报的ROE

这里有个小的问题，或者说关键点：
就是A股票和B股票，实际上比的不是同一个时间的东东，
A股票是去年的12个月TTM的ROE，
B股票则是去年1季度~今年1季度12个月的TTM，
他俩没有完全对齐，差了1个季度，
我自己觉得，可能这是"最优"的比较了把，毕竟，我是不能用"未来"数据的，
我站在4.15日，能看到的A股票的信息，就是去年他的ROE_TTM，虽然滞后了1个季度，但是总比没有强。
1个季度的之后，忍了。

这个确实是问题，我梳理一下问题，虽然解决不了，但是至少要自己门清：
- 多只股票的ROE_TTM都会滞后，最多可能会之后3-4个月（比如4.30号才得到去年的年报）
- 多只股票可能都无法对齐ROE_TTM，比如上例中A用的是当期的，而极端的D，用的居然是截止去年10.30号发布的9.30号的3季报的数据了

## [净资产收益率变动ROE_YOY](https://github.com/piginzoo/mfm_learner/tree/main/example/factors/roe.py)

ROEYoY：ROE Year over Year，同比增长率、ROE变动。

注：[关于ROE计算](https://baike.baidu.com/item/%E5%87%80%E8%B5%84%E4%BA%A7%E6%94%B6%E7%9B%8A%E7%8E%87)

遇到一个问题：

    SELECT ann_date,end_date,roe_yoy
    FROM tushare.fina_indicator
    where ts_code='600000.SH' and ann_date='20180428'
    ------------------------------------------------------
    '20180428','20180331','-12.2098'
    '20180428','20171231','-11.6185'
    ------------------------------------------------------

可以看出来，用code+datetime作为索引，可以得到2个roe_yoy，这是因为，一个是年报和去年同比，一个是季报和去年同比，
而且，都是今天发布的，所以，这里的我们要理解，yoy，同比，比的可能是同季度的，也可能是半年的，一年的，
如果严格的话，应该使用roe_ttm，然后去和去年的roe_ttm比较，这样最准，但是，这样处理就太复杂了，

所以，折中的还是用甲股票和乙股票，他们自己的当日年报中提供的yoy同比对比，比较吧。
这种折中，可能潜在一个问题，就是我可能用我的"季报同比"，对比了，你的"年报同比"，原因是我们同一日、相近日发布的不同scope的财报，
这是一个问题，最好的解决办法，还是我用我的ROE_TTM和我去年今日的ROE_TTM，做同比；然后，再和你的结果比，就是上面说的方法。

算了，还是用这个这种方法吧，
所以，上述的问题，是我自己（甲股票）同一天发了2份财报（年报和季报），这个时候我取哪个yoy同比结果呢，
我的解决之道是，随便，哪个都行

## 资产收益率ROA_TTM

## 资产收益率变动ROA_YOY

## [税息折旧及摊销前利润EBITDA](https://github.com/piginzoo/mfm_learner/tree/main/example/factors/ebitda.py)

参考：[税息折旧及摊销前利润，简称EBITDA](https://baike.baidu.com/item/EBITDA/7810909)

税息折旧及摊销前利润，简称EBITDA，是Earnings Before Interest, Taxes, Depreciation and Amortization的缩写，
即未计利息、税项、折旧及摊销前的利润。

EBITDA受欢迎的最大原因之一是，EBITDA比营业利润显示更多的利润，公司可以通过吹捧EBITDA数据，把投资者在高额债务和巨大费用上面的注意力引开。

EBITDA = EBIT【息税前利润】 - Taxation【税款】+ Depreciation & Amortization【折旧和摊销】
- EBIT = 净利润+利息+所得税-公允价值变动收益-投资收益
- 折旧和摊销 = 资产减值损失

EBITDA常被拿来和现金流比较，因为它和净收入之间的差距就是两项对现金流没有影响的开支项目， 即折旧和摊销。
> 意思是说，这玩意，只考虑跟当期钱相关的，包含了利息，而且，刨除了那些假装要扣的钱（摊销和折旧）

我和MJ讨论后，我的理解是：

咱不把借债的因素考虑进来，也不考虑缴税呢，也不让折旧摊销来捣乱，我就看我的赚钱能力（你丫甭管我借钱多少，咱就看，干出来的粗利润）
当然是刨除了生产、销售成本之后的，不考虑息税和折旧。

查了tushare，有两个地方可以提供ebitda，一个是

`pro.fina_indicator(ts_code='600000.SH',fields='ts_code,ann_date,end_date,ebit,ebitda')`

另外一个是，

`pro.income(ts_code='600000.SH',fields='ts_code,ann_date,end_date,ebit,ebitda')`

income接口，是提供有效数据，fina_indicator提供的都是None，靠，tushare没有认真去清洗啊。

不过，我又去[聚宽](https://www.joinquant.com/help/api/help#factor_values:%E5%9F%BA%E7%A1%80%E5%9B%A0%E5%AD%90) ，
他的《基础因子》api中，有ebotda，而且很全，是按照每日给出的，很棒。
唉，可惜他太贵了，他的API使用license是6000+/年（tushare 200/年）
更可怕的是，tushare的income的结果，和聚宽的基础因子接口，出来的结果是!!!不一样!!!的，不信可以自己去试一试（聚宽只能在他的在线实验室测试）
我信谁？
除非我去查年报，自己按照上面的"息税前利润（EBIT）+折旧费用+摊销费用 =EBITDA" 公式自己算一遍，好吧，逼急了，我就这么干。

目前，基于我只能用tushare的数据，我选择用income接口，然后按照交易日做ffill，我留个 TODO，将来这个指标要做进一步的优化！

# 资产本结构

## [资产负债率AssetsDebtRate](https://github.com/piginzoo/mfm_learner/tree/main/example/factors/assets_debt_ratio.py)

## [市值](https://github.com/piginzoo/mfm_learner/tree/main/example/factors/market_value.py)
对总市值取了一个对数，减少差异性。

参考：https://zhuanlan.zhihu.com/p/161706770

## [流通市值](https://github.com/piginzoo/mfm_learner/tree/main/example/factors/market_value.py)
对流通市值取了一个对数，减少差异性。

# 技术因子

## [动量因子Momentum](https://github.com/piginzoo/mfm_learner/tree/main/example/factors/momentum.py)
动量因子，动量因子是指与股票的价格和交易量变化相关的因子，常见的动量因子：一个月动量、3个月动量等。
计算个股（或其他投资标的）过去N个时间窗口的收益回报：
```
adj_close = (high + low + close)/3
adj_return = adj_close_t - adj_close_{t-n}
```
来计算受盘中最高价和最低价的调整的调整收盘价动量，逻辑是，在日线的层面上收盘价表示市场主力资本对标的物的价值判断，
而最高价和最低价往往反应了市场投机者的情绪，同时合理考虑这样的多方情绪可以更好的衡量市场的动量变化。

计算动量，动量，就是往前回溯period个周期，然后算收益，
由于股票的价格是一个随经济或标的本身经营情况有变化的变量。那么如果变量有指数增长趋势（exponential growth），
比如 GDP，股票价格，期货价格，则一般取对数，使得 lnGDP 变为线性增长趋势（linear growth），
为了防止有的价格高低，所以用log方法，更接近，参考：https://zhuanlan.zhihu.com/p/96888358

本来的动量用的是减法，这里，换成了除法，就是用来解决文中提到的规模不同的问题    
    
参考：
- https://zhuanlan.zhihu.com/p/96888358
- https://zhuanlan.zhihu.com/p/379269953

## [CLV因子](https://github.com/piginzoo/mfm_learner/tree/main/example/factors/clv.py)
clv: close location value,

`( (close-day_low) - (day_high - close) ) / (day_high - day_low)`

这玩意，是一个股票的，每天都有，是一个数，
我们要从一堆的股票N只中，得到N个这个值，可以形成一个截面，
用这个截面，我们可以拟合出β和α，
然后经过T个周期（交易日），就可以有个T个β和α，
因子长这个样子：
```
trade_date
 	      	000001.XSHE  000002.XSHE
2019-01-02  -0.768924    0.094851    
```
类比gpd、股指（市场收益率），这个是因子不是一个啊？而是多个股票N个啊？咋办？

参考：https://www.bilibili.com/read/cv13893224?spm_id_from=333.999.0.0

## [波动率因子Std](https://github.com/piginzoo/mfm_learner/tree/main/example/factors/std.py)

[波动率因子](https://zhuanlan.zhihu.com/p/30158144)：
波动率因子有很多，我这里的是std，标准差，
而算标准差，又要设置时间窗口，这里设定了10，20，60，即半个月、1个月、3个月

## [特质波动率因子ivff](https://github.com/piginzoo/mfm_learner/tree/main/example/factors/ivff.py)
所谓"特质波动率"： 就是源于一个现象"低特质波动的股票，未来预期收益更高"。

参考：
- https://www.joinquant.com/view/community/detail/b27081ecc7bccfc7acc484f8a63e2459
- https://www.joinquant.com/view/community/detail/1813dae5165ee3c5c81e2408d7fe576f
- https://zhuanlan.zhihu.com/p/30158144
- https://zhuanlan.zhihu.com/p/379585598
- https://mp.weixin.qq.com/s/k_2ltrIQ7jkgAKhDc7Vo2A
- https://blog.csdn.net/FightingBob/article/details/106791144
- https://uqer.datayes.com/v3/community/share/58db552a6d08bb0051c52451


特质波动率(Idiosyncratic Volatility, IV)与预期收益率的负向关系既不符合经典资产定价理论，
也不符合基于不完全信息的定价理论，因此学术界称之为“特质波动率之谜”。

该因子虽在多头部分表现略逊于流通市值因子，但在多空方面表现明显强于流通市值因子，
说明特质波动率因子具有很好的选股区分能力，并且在空头部分有良好的风险警示作用。

基于CAPM的特质波动率 IVCAPM: 就是基于CAMP的残差的年化标准差来衡量。
基于Fama-French三因子模型的特质波动率 IVFF3： 就是在IVCAMP的基础上，再剔除市值因子和估值因子后的残差的年化标准差来衡量。

说说实现，
特质波动率，可以有多种实现方法，可以是CAMP市场的残差，也可以是Fama-Frech的残差，这里，我用的是FF3的残差，
啥叫FF3的残差，就是，用Fama定义的模型，先去算因子收益，[参考](https://github.com/piginzoo/mfm_learner/tree/main/fama/factor.py)中，
使用股票池（比如中证500），去算出来的全市场的SMB，HML因子，

然后，就可以对某一直股票，比如"招商银行"，对他进行回归：r_i = α_i + b1 * r_m_i + b2 * smb_i + b3 * hml_i + e_i
我们要的就是那个e_i，也就是这期里，无法被模型解释的'**特质**'。上式计算，用的是每天的数据，为何强调这点呢，是为了说明e_i的i，指的是每天。
那波动率呢？

就是计算你回测周期的内的标准差 * sqrt(T)，比如你回测周期是20天，那就是把招商银行这20天的特异残差求一个标准差，然后再乘以根号下20。
这个值，是这20天共同拥有的一个"特异波动率"，对，这20天的因子暴露值，都一样，都是这个数！
我是这么理解的，也不知道对不对，这些文章云山雾罩地不说人话都。

年2季报累计值) + 2016Q4 (2016年4季度累计值) - 2016Q2 (2016年2季度累计值)

## [换手率因子TurnOver](https://github.com/piginzoo/mfm_learner/tree/main/example/factors/turnover_rate.py)

换手率因子：

- https://zhuanlan.zhihu.com/p/37232850
- https://crm.htsc.com.cn/doc/2017/10750101/6678c51c-a298-41ba-beb9-610ab793cf05.pdf  华泰~换手率类因子
- https://uqer.datayes.com/v3/community/share/5afd527db3a1a1012acad84c

换手率因子是一类很重要的情绪类因子，反映了一支股票在一段时间内的流动性强弱，和持有者平均持有时间的长短。
一般来说换手率因子的大小和股票的收益为负向关系，即换手率越高的股票预期收益越低，换手率越低的股票预期收益越高。

四个构造出来的换手率类的因子（都是与股票的日均换手率相关）：
- turn_Nm：个股最近N个月的日均换手率，表现了个股N个月内的流动性水平。N=1,3,6
- bias_turn_Nm：个股最近N个月的日均换手率除以个股两年内日均换手率再减去1，代表了个股N个月内流动性的乖离率。N=1,3,6
- std_turn_Nm：个股最近N个月的日换手率的标准差，表现了个股N个月内流动性水平的波动幅度。N=1,3,6
- bias_std_turn_Nm：个股最近N个月的日换手率的标准差除以个股两年内日换手率的标准差再减去1，代表了个股N个月内流动性的波动幅度的乖离率。N=1,3,6

这是4个因子哈，都是跟换手率相关的，他们之间具备共线性，是相关的，要用的时候，挑一个好的，或者，做因子正交化后再用。

市值中性化：换手率类因子与市值类因子存在一定程度的负相关性，我们对换手率因子首先进行市值中性化处理，从而消除了大市值对于换手率因子表现的影响。

知乎文章的结论：进行市值中性化处理之后，因子表现有明显提高。在本文的回测方法下，turn_1m和std_turn_1m因子表现较好。



