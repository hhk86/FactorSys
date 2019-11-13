import re
import os
import configparser
from common.db.__core.connector import SqlConnector,NoSqlConnector
from collections import defaultdict
from common import default_logger_decorator,log

def replace_more_space(s):
    return re.sub(r"\s{2,}"," ",s.strip())

def replace_space(s):
    return re.sub(r"\s","",s)

@default_logger_decorator
def init_db_config():
    '''
    根据configs/db_config.ini配置文件中的数据库配置进行各库初始化
    :return: dbs        dict   数据库检索名：数据库连接池
             only_read  list    无写权限数据库列表
    '''
    dbs = defaultdict()
    only_read = list()
    config = configparser.ConfigParser()
    config.read('%s/../../../configs/db_config.ini' % os.path.dirname(__file__))
    dbname_list = config.sections()
    log.info(u'引自%s配置文件的数据库配置项：%s' % ('configs/db_config.ini',dbname_list))
    for dbname in dbname_list:
        params = dict(config.items(dbname))
        if params.get('database.write').lower() != 'true':
            only_read.append(dbname)
        dbs.update({dbname: NoSqlConnector(params) if params.get('database.nosql').lower() == 'true' else SqlConnector(params)})
        log.info(u"%s数据库连接初始化成功" % dbname)
    return dbs,only_read