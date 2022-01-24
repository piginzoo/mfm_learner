import inspect


def __get_value(name, arg_names, args_values):
    """
    按照名字，得到对应的值
    """
    if name not in arg_names:
        raise ValueError(name + " 不在方法签名中：" + arg_names)
    return args_values[arg_names.index(name)]


def __cache(name):
    def cache(func):
        def cache_it(*args, **kwargs):
            print("调用者的名字：", func.__name__)
            print("调用者的名字：", inspect.signature(func))
            signature = inspect.signature(func)
            arg_names = [k for k in signature.parameters.keys()]
            stock_code = __get_value('stock_code', arg_names, args)
            start_date = __get_value('start_date', arg_names, args)
            end_date = __get_value('end_date', arg_names, args)
            print("stock_code,start_date,end_date：", stock_code, start_date, end_date)
            print('装饰器传入参数为：{}'.format(name))
            print(args)
            print(kwargs)
            print('所有传入的参数========>', list(args)+list(kwargs.values()))
            result = func(*args, **kwargs)
            print("在函数执行后，做一些操作")
            return result

        return cache_it

    return cache


@__cache("保存")
def test(stock_code, start_date, end_date, fields='F', field2='G'):
    print("我是函数我是函数！！！")


# python test_wrapper.py
if __name__ == '__main__':
    test("A", "B", "C",fields='D')
