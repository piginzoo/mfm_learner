from functools import partial
import pandas as pd
from scipy import linalg

"""
做多因子合成，对筛选后的因子进行组合，一般有以下常规处理：
- 因子间存在较强同质性时，先使用施密特正交化方法对因子做正交化处理，用得到的正交化残差作为因子（也可以不使用，正交化会破坏因子的经济学逻辑，并剔除一些信息）
- 因子组合加权，常规的方法有：等权重、以某个时间窗口的滚动平均ic为权重、以某个时间窗口的滚动ic_ir为权重、最大化上个持有期的ic_ir为目标处理权重、最大化上个持有期的ic为目标处理权重
注:因为计算IC需要用到下一期股票收益,因此在动态加权方法里，实际上使用的是前一期及更早的IC值(向前推移了holding_period)计算当期的权重
参考：
- https://github.com/ChannelCMT/QTC_2.0/blob/master/6_%E5%9B%A0%E5%AD%90%E7%A0%94%E5%8F%91%E5%B7%A5%E5%85%B7%E5%AE%9E%E6%93%8DRichard/Section_3%20%E5%9B%A0%E5%AD%90%E9%A2%84%E5%A4%84%E7%90%86%E6%96%B9%E6%B3%95%E3%80%81%E5%A4%9A%E5%9B%A0%E5%AD%90%E5%90%88%E6%88%90.ipynb
- https://github.com/xingetouzi/jaqs-fxdayu/blob/master/jaqs_fxdayu/research/signaldigger/multi_factor.py
"""


def synthesize(factors: list):
    # 因子间存在较强同质性时，使用施密特正交化方法对因子做正交化处理，用得到的正交化残差作为因子
    new_factors = orthogonalize(factors_dict=factor_dict,
                                standardize_type="rank",
                                # 输入因子标准化方法，有"rank"（排序标准化）,"z_score"(z-score标准化)两种（"rank"/"z_score"）
                                winsorization=False,  # 是否对输入因子去极值
                                index_member=index_member)  # 是否只处理指数成分股



# 因子间存在较强同质性时，使用施密特正交化方法对因子做正交化处理，用得到的正交化残差作为因子
def orthogonalize(factors_dict=None, standardize_type="z_score", winsorization=False, index_member=None):
    """
    # 因子间存在较强同质性时，使用施密特正交化方法对因子做正交化处理，用得到的正交化残差作为因子
    :param index_member:
    :param factors_dict: 若干因子组成的字典(dict),形式为:
                         {"factor_name_1":factor_1,"factor_name_2":factor_2}
                       　每个因子值格式为一个pd.DataFrame，索引(index)为date,column为asset
    :param standardize_type: 标准化方法，有"rank"（排序标准化）,"z_score"(z-score标准化)两种（"rank"/"z_score"）
    :return: factors_dict（new) 正交化处理后所得的一系列新因子。
    """

    def Schmidt(data):
        return linalg.orth(data)

    def get_vector(date, factor):
        return factor.loc[date]

    if not factors_dict or len(list(factors_dict.keys())) < 2:
        raise ValueError("你需要给定至少２个因子")

    new_factors_dict = {}  # 用于记录正交化后的因子值
    for factor_name in factors_dict.keys():
        new_factors_dict[factor_name] = []
        # 处理非法值
        factors_dict[factor_name] = jutil.fillinf(factors_dict[factor_name])
        factors_dict[factor_name] = process._mask_non_index_member(factors_dict[factor_name],
                                                                   index_member=index_member)
        if winsorization:
            factors_dict[factor_name] = process.winsorize(factors_dict[factor_name])

    factor_name_list = list(factors_dict.keys())
    factor_value_list = list(factors_dict.values())

    # 施密特正交
    for date in factor_value_list[0].index:
        data = list(map(partial(get_vector, date), factor_value_list))
        data = pd.concat(data, axis=1, join="inner")
        data = data.dropna()
        if len(data) == 0:
            continue
        data = pd.DataFrame(Schmidt(data), index=data.index)
        data.columns = factor_name_list
        for factor_name in factor_name_list:
            row = pd.DataFrame(data[factor_name]).T
            row.index = [date, ]
            new_factors_dict[factor_name].append(row)

    # 因子标准化
    for factor_name in factor_name_list:
        factor_value = pd.concat(new_factors_dict[factor_name])
        # 恢复在正交化过程中剔除的行和列
        factor_value = factor_value.reindex(index=factor_value_list[0].index, columns=factor_value_list[0].columns)
        if standardize_type == "z_score":
            new_factors_dict[factor_name] = process.standardize(factor_value, index_member)
        else:
            new_factors_dict[factor_name] = process.rank_standardize(factor_value, index_member)

    return new_factors_dict


def combine_factors(factors_dict=None,
                    standardize_type="rank",
                    winsorization=False,
                    index_member=None,
                    weighted_method="equal_weight",
                    props=None):
    """
    # 因子组合
    :param index_member:　是否是指数成分 pd.DataFrame
    :param winsorization: 是否去极值
    :param props:　当weighted_method不为equal_weight时　需传入此配置　配置内容包括
     props = {
            'price': None,
            'daily_ret':None,
            'high': None,
            'low': None,
            'ret_type': 'return',
            'benchmark_price': None,
            'daily_benchmark_ret':None,
            'period': 5,
            'mask': None,
            'can_enter': None,
            'can_exit': None,
            'forward': True,
            'commission': 0.0008,
            "covariance_type": "simple",  # 还可以为"shrink"
            "rollback_period": 120
        }
    :param factors_dict: 若干因子组成的字典(dict),形式为:
                         {"factor_name_1":factor_1,"factor_name_2":factor_2}
                       　每个因子值格式为一个pd.DataFrame，索引(index)为date,column为asset
    :param standardize_type: 标准化方法，有"rank"（排序标准化）,"z_score"(z-score标准化),为空则不进行标准化操作
    :param weighted_method 组合方法，有equal_weight,ic_weight, ir_weight, max_IR.若不为equal_weight，则还需配置props参数．
    :return: new_factor 合成后所得的新因子。
    """

    def generate_props():
        props = {
            'price': None,
            'daily_ret': None,
            'high': None,
            'low': None,
            'ret_type': 'return',
            'benchmark_price': None,
            'daily_benchmark_ret': None,
            'period': 5,
            'mask': None,
            'can_enter': None,
            'can_exit': None,
            'forward': True,
            'commission': 0.0008,
            "covariance_type": "simple",  # 还可以为"shrink"
            "rollback_period": 120
        }
        return props

    def standarize_factors(factors):
        if isinstance(factors, pd.DataFrame):
            factors_dict = {"factor": factors}
        else:
            factors_dict = factors
        factor_name_list = factors_dict.keys()
        for factor_name in factor_name_list:
            factors_dict[factor_name] = jutil.fillinf(factors_dict[factor_name])
            factors_dict[factor_name] = process._mask_non_index_member(factors_dict[factor_name],
                                                                       index_member=index_member)
            if winsorization:
                factors_dict[factor_name] = process.winsorize(factors_dict[factor_name])
            if standardize_type == "z_score":
                factors_dict[factor_name] = process.standardize(factors_dict[factor_name])
            elif standardize_type == "rank":
                factors_dict[factor_name] = process.rank_standardize(factors_dict[factor_name])
            elif standardize_type is not None:
                raise ValueError("standardize_type 只能为'z_score'/'rank'/None")
        return factors_dict

    def _cal_weight(weighted_method="ic_weight"):
        _props = generate_props()
        if not (props is None):
            _props.update(props)
        if _props["price"] is None and _props["daily_ret"] is None:
            raise ValueError("您需要在配置props中提供price或者daily_ret")

        if weighted_method == "factors_ret_weight":
            factors_ret = get_factors_ret_df(factors_dict=factors_dict,
                                             **_props)
            return factors_ret_weight(factors_ret,
                                      _props['period'],
                                      _props["rollback_period"])
        else:
            # 此处计算的ic,用到的因子值是shift(1)后的
            # t日ic计算逻辑:t-1的因子数据，t日决策买入，t+n天后卖出对应的ic
            ic_df = get_factors_ic_df(factors_dict=factors_dict,
                                      **_props)
            if weighted_method == 'max_IR':
                return max_IR_weight(ic_df,
                                     _props['period'],
                                     _props["rollback_period"],
                                     _props["covariance_type"])
            elif weighted_method == "ic_weight":
                return ic_weight(ic_df,
                                 _props['period'],
                                 _props["rollback_period"])
            elif weighted_method == "ir_weight":
                return ir_weight(ic_df,
                                 _props['period'],
                                 _props["rollback_period"])
            elif weighted_method == "max_IC":
                # 计算t期因子ic用的是t-1期因子，所以要把因子数据shift(1)
                shift_factors = {
                    factor_name: factors_dict[factor_name].shift(1) for factor_name in factors_dict.keys()
                }
                return max_IC_weight(ic_df,
                                     shift_factors,
                                     _props['period'],
                                     _props["covariance_type"])

    def sum_weighted_factors(x, y):
        return x + y

    if not factors_dict or len(list(factors_dict.keys())) < 2:
        raise ValueError("你需要给定至少2个因子")
    factors_dict = standarize_factors(factors_dict)

    if weighted_method in ["max_IR", "max_IC", "ic_weight", "ir_weight", "factors_ret_weight"]:
        weight = _cal_weight(weighted_method)
        weighted_factors = {}
        factor_name_list = factors_dict.keys()
        for factor_name in factor_name_list:
            w = pd.DataFrame(data=weight[factor_name], index=factors_dict[factor_name].index)
            w = pd.concat([w] * len(factors_dict[factor_name].columns), axis=1)
            w.columns = factors_dict[factor_name].columns
            weighted_factors[factor_name] = factors_dict[factor_name] * w
    elif weighted_method == "equal_weight":
        weighted_factors = factors_dict
    else:
        raise ValueError('weighted_method 只能为equal_weight, ic_weight, ir_weight, max_IR, max_IC, factors_ret_weight')
    new_factor = reduce(sum_weighted_factors, weighted_factors.values())
    new_factor = standarize_factors(new_factor)["factor"]
    return new_factor
