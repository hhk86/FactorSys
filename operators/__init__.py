'''
开发因子计算程序时，只需在operators文件夹下增加一个py文件，文件中的代码按下述规则开发：
1、所有因子计算程序需继承operator.Operator抽象类，并实现load_data,fit,dump_data三个抽象函数，execute、init_date_list函数可视使用情况进行重写。
2、默认的execute执行接口要求由父类继承的三个抽象函数的实现满足以下流式调用规则：dump_data(fit(load_data(date,code_list)))
3、load_data作为第一入口，要求满足以下形参规则：
        参数名         类型      含义
        date           string    计算date期间的因子数据
        code_list      list      默认为None即计算全部A股股票，待计算因子的股票代码列表
4、根据需求也可自行重写execute，但形参依旧满足3中所示。
5、所有因子计算程序只需from operators.operator import Operator，并按1规则实现具体因子类即可。所有数据库包(common.db)、日志包(common.log)、工具包(utils)
    、系统参数(common.conf_dict)均在Operator抽象父类中进行了初始化，可通过cls获取。
6、所有因子计算程序间默认不可存在依赖关系，如因子A不能依赖因子B的输出结果进行二次计算。
'''
import importlib
from common import default_logger_decorator,log

@default_logger_decorator
def execute(module_name):
    module = importlib.import_module("operators.%s" % module_name[:module_name.rindex('.')])
    cls = getattr(module, module_name[module_name.rindex('.')+1:])
    for date in cls.init_date_list():
        log.info(u'---------------------------%s在%s时的因子数据计算开始----------------------------' % (cls.__name__,date))
        cls.execute(date)
        log.info(u'---------------------------%s在%s时的因子数据计算结束----------------------------' % (cls.__name__,date))

