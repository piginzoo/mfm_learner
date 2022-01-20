from functools import wraps

def __cache(name):
    def cache(func):
        @wraps(func)
        def cache_it(*args, **kwargs):
            print('装饰器传入参数为：{}'.format(name))
            print('在函数执行前，做一些操作',*args)
            result = func(*args, **kwargs)
            print("在函数执行后，做一些操作")
            return result
        return cache_it
    return cache
@__cache("保存")
def test(test1,test2,test3):
    print(test1,test2,test3)


def test_yield():
    data=list(range(10))
    for d in data:
        yield str(d)+"_"+str(d)

# python test_wrapper.py
if __name__ == '__main__':
    test("A","B","C")
    data = test_yield()
    for a in data:
        print(a)