import importlib
import datetime
from common import default_logger_decorator,log

@default_logger_decorator
def execute(module_name):
    module = importlib.import_module("services.%s" % module_name[:module_name.rindex('.')])
    cls = getattr(module, module_name[module_name.rindex('.')+1:])
    date = datetime.datetime.now().strftime("%Y%m%d")
    log.info(u'---------------------------%s在%s时的服务计算开始----------------------------' % (cls.__name__,date))
    cls.execute(date)
    log.info(u'---------------------------%s在%s时的服务计算结束----------------------------' % (cls.__name__,date))

