'''
日志模块，初始化日志，设置文件输出和控制台输出两个handler。
后期根据具体需求可增加向mongodb数据库的日志输出
使用方式：
from common import log
log.info(r"日志内容")
或使用装饰器：
default_logger_decorator装饰器，默认只提供错误日志打印
operator_logger_decorator装饰器，专用于Operator抽象类及其子类的类方法使用，除错误日志外，提供函数进入和结束日志打印
'''
import configparser
import logging
import datetime
import os
import sys
from concurrent_log_handler import ConcurrentRotatingFileHandler
#上linux环境需要替换为下面的ConcurrentRotatingFileHandler
# from cloghandler import ConcurrentRotatingFileHandler

__config = configparser.ConfigParser()
__config.read('%s/../configs/sys_config.ini' % os.path.dirname(__file__))
__logname = '%s/factorSys_%s.log' % (__config.get('logger', 'logger.path'), datetime.datetime.now().strftime('%Y-%m-%d'))
__fmt = logging.Formatter('[%(asctime)s][Process ID:%(process)d][%(pathname)s][line:%(lineno)d][%(levelname)s] %(message)s', '%Y-%m-%d %H:%M:%S')

# 日志输出到文件
__handle1 = ConcurrentRotatingFileHandler(__logname, maxBytes=1024 * 1024 * int(__config.get('logger', 'logger.size')), backupCount=int(__config.get('logger', 'logger.num')), encoding=__config.get('logger', 'logger.encoding'))
__handle1.setFormatter(__fmt)
# 输出到屏幕
__handle2 = logging.StreamHandler(stream=sys.stdout)
__handle2.setFormatter(__fmt)

log = logging.getLogger()
log.setLevel(__config.get('logger', 'logger.level').upper())
log.addHandler(__handle1)
log.addHandler(__handle2)


def default_logger_decorator(func):
    '''
    默认日志装饰器，提供错误日志打印
    '''
    def inner(*args,**kargs):
        try:
            res = func(*args,**kargs)
            return res
        except Exception as e:
            log.error(e, exc_info=1)
            raise e
    return inner

def operator_logger_decorator(func):
    '''
    Operator及其子类的专用日志装饰器，提供错误日志打印，函数进入和结束打印
    '''
    def inner(*args,**kargs):
        try:
            operator_name = args[0].__name__
            log.info(u'%s.%s开始执行 : args=%s, kargs=%s' % (operator_name,func.__name__,args,kargs))
            res = func(*args,**kargs)
            return res
        except Exception as e:
            log.error(e,exc_info=1)
            raise e
        finally:
            log.info(u'%s.%s结束执行' % (operator_name,func.__name__))
    return inner