'''
缓存模块
    目前缓存模块中只有services_cache缓存器。并在sys_config.ini的caches中进行了配置，配置项为cache.services。
    配置项的值为逗号隔开的配置信息(结尾无逗号)，如cache.services = barra_cne5_service.BarraCNE5Service,barra_cne5_service2.BarraCNE5Service2
    缓存器会根据配置信息使用逗号分隔，然后顺序的调用其指定类的cache方法进行缓存加载。
    此外，缓存器使用BackgroundScheduler的scheduled_job进行注解装饰，并配置调用频率。

使用方式：
    在程序执行入口py中import common.cache即可，不引入模块不会执行
'''
import os
import configparser
import importlib
import datetime
from common import log, default_logger_decorator
from apscheduler.schedulers.background import BackgroundScheduler

__caches_conf = configparser.ConfigParser()
__caches_conf.read('%s/../configs/sys_config.ini' % os.path.dirname(__file__))
__caches_conf = __caches_conf['caches']

__sched = BackgroundScheduler()


@default_logger_decorator
@__sched.scheduled_job('cron', id='services_cache', hour=7, minute=0)#每天的07：00：00刷新缓存，非线程安全模式
def __services_cache():
    log.info("------------------------services_before缓存器内存刷新开始---------------------")
    items = __caches_conf.get('cache.services').split(',')
    date = datetime.datetime.now().strftime('%Y%m%d')
    for module_name in items:
        module = importlib.import_module("services.%s" % module_name[:module_name.rindex('.')])
        cls = getattr(module, module_name[module_name.rindex('.') + 1:])
        cls.cache(date)
    log.info("------------------------services_before缓存器内存刷新结束---------------------")


__sched.start()
__services_cache()
