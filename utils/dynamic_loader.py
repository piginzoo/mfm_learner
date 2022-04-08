import importlib
import inspect
import logging
from pkgutil import walk_packages

from utils import utils

logger = logging.getLogger(__name__)


def create_factor_by_name(name, factor_dict):
    for _, clazz in factor_dict.items():
        factor = clazz()
        factor_name = factor.name()
        if type(factor_name) == list and name in factor_name: return factor
        if factor.name() == name: return factor
    logger.warning("无法根据名称[%s]创建因子实例",name)
    raise ValueError(f"无法根据名称{name}创建因子实例")


# 对构造函数的参数做类型转换，目前仅支持int，未来可以自由扩充
# 注意！构造函数的参数一定要标明类型，如 name:int
def convert_params(clazz, param_values):
    # logger.debug("准备转化%r的参数：%r",clazz,param_values)
    full_args = inspect.getfullargspec(clazz.__init__)
    annotations = full_args.annotations
    arg_names = full_args.args[1:]  # 第一个是self，忽略
    new_params = []
    for i, value in enumerate(param_values):

        arg_name = arg_names[i]
        arg_type = annotations.get(arg_name, None)
        if arg_type and value and arg_type == int:
            logger.debug("参数%s的值%s转化成int", arg_name, value)
            value = int(value)
        new_params.append(value)

    return new_params


def dynamic_load_classes(module_name, parent_class):
    classes = []
    base_module = importlib.import_module(module_name)

    for _, name, is_pkg in walk_packages(base_module.__path__, prefix="{}.".format(module_name)):
        if is_pkg: continue

        module = importlib.import_module(name)

        for name, obj in inspect.getmembers(module):

            if not inspect.isclass(obj): continue
            if not issubclass(obj, parent_class): continue
            if obj == parent_class: continue
            classes.append(obj)

            # print(name, ",", obj)

    return classes


def dynamic_instantiation(package_name, parent_class):
    objs = {}
    classes = dynamic_load_classes(package_name, parent_class)
    for clazz in classes:
        # obj = clazz()
        # print(clazz.__name__)
        objs[clazz.__name__] = clazz
    return objs


# python -m utils.dynamic_loader
if __name__ == "__main__":
    utils.init_logger()
    from example.factors.factor import Factor

    class_dict = dynamic_instantiation("example.factors", Factor)
    logger.debug("所有的加载类：%r", class_dict)

    # obj = get_validator("KeyValueValidator(签发机关)", class_dict)
    # assert obj.process("机关哈哈哈哈哈哈") is not None
    #
    # # 测试类名拼错了
    # obj = get_validator("AchiveNoValidator(档案编号)", class_dict)
    # assert obj is None
    #
    # obj = get_validator("AchieveNoValidator(档案编号)", class_dict)
    # assert obj is not None
    #
    # obj = get_validator("VehicleUseCharacterValidator()", class_dict)
    # assert obj is not None
