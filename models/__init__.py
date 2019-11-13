import importlib
from common import default_logger_decorator,log

@default_logger_decorator
def execute(module_name):
    module = importlib.import_module("models.%s" % module_name[:module_name.rindex('.')])
    cls = getattr(module, module_name[module_name.rindex('.')+1:])
    for date in cls.init_date_list():
        log.info(u'---------------------------%s在%s时的模型计算开始----------------------------' % (cls.__name__,date))
        cls.execute(date)
        log.info(u'---------------------------%s在%s时的模型计算结束----------------------------' % (cls.__name__,date))

