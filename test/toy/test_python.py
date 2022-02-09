"""
测试一些基础的语法
"""

class A():
    def __init__(self):
        print("i am parent, i should be print out")

class B(A):
    pass

b = B()

# python -m test.toy.test_python