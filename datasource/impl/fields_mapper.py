"""
字段不一样，统一成一样的，原则：
- 尽量向tushare靠拢，毕竟，它是我的主力数据源
- 实在不合理的，也要改，比如ts_code=>code

懒得做成配置文件了，直接用个python文件，挺好
"""
MAPPER={
    'tushare':{
        'ts_code':'code',
        'vol': 'volume',
        'trade_date': 'datetime'
    },
    'baostock':{

    },
    'database':{

    }
}