import sys
def abc():
    name = sys._getframe(1).f_code.co_name
    print("%s is call me" % name)

def call_func():
    abc()

# python test_callname.py
if __name__ == '__main__':
    call_func()