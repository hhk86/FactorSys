import pandas as pd
from sqlalchemy.sql import text
from common.db.__core.utils import replace_more_space,replace_space,init_db_config
from common import log

__DBs, __only_read = init_db_config()

def __get_DBconnector(dbname):
    conn = __DBs.get(dbname)
    if not conn:
        raise Exception(u'dbname参数错误')
    return conn

def __write_permission(dbname):
    '''
    写操作权限控制
    :param dbname:
    :return:
    '''
    if dbname in __only_read:
        raise Exception(u'非法数据库写入操作')

def __truncat_permission(dbname, tableName):
    '''
    表清空操作权限控制，暂不支持任意表的数据清空
    :param dbname:
    :param tableName:
    :return:
    '''
    raise Exception(u'非法的清空操作')

def query(dbname,sql):
    if replace_more_space(sql.lower()).find('select') != 0:
        raise Exception(u"sql语句有误")
    # log.info(u"执行sql：%s" % sql)
    return pd.io.sql.read_sql(sql, __get_DBconnector(dbname).get_engine())

def update(dbname, sql):
    __write_permission(dbname)
    if replace_more_space(sql.lower()).find('update') != 0:
        raise Exception(u"sql语句有误")
    log.info(u"执行sql：%s" % sql)
    with __get_DBconnector(dbname).get_session() as s:
        s.execute(text(sql))

def insert(dbname, tableName, data):
    __write_permission(dbname)
    log.debug(u"插入数据：%s" % data)
    log.info(u"插入数据%s行" % data.shape[0])
    pd.io.sql.to_sql(data, replace_space(tableName), __get_DBconnector(dbname).get_engine(),__get_DBconnector(dbname).get_schema(), if_exists='append', index=False, chunksize=10000)

def delete_data(dbname, sql):
    __write_permission(dbname)
    if replace_more_space(sql.lower()).find('delete from') != 0:
        raise Exception(u"sql语句有误")
    log.info(u"执行sql：%s" % sql)
    with __get_DBconnector(dbname).get_session() as s:
        s.execute(text(sql))

def truncate(dbname, tableName):
    __write_permission(dbname)
    __truncat_permission(dbname,tableName)
    sql = r'truncate %s ' % replace_space(tableName)
    log.info(u"执行sql：%s" % sql)
    with __get_DBconnector(dbname).get_session() as s:
        s.execute(text(sql))

def is_table_exist(dbname, tableName):
    sql = "show tables like '%s'" % replace_space(tableName)
    log.info(u"执行sql：%s" % sql)
    with __get_DBconnector(dbname).get_session() as s:
        rs = s.execute(text(sql))
    return False if ((rs is None) or (rs.rowcount<1)) else True

def query_MongoDB(dbname, database, collection, *args):
    coll = __get_DBconnector(dbname).get_engine()[database][collection]
    log.info(u"执行MongoDB查询操作：%s" % str(args))
    cursor = coll.find(*args)
    return pd.DataFrame(list(cursor))

def insert_to_MongoDB(dbname, database, collection, data):
    __write_permission(dbname)
    coll = __get_DBconnector(dbname).get_engine()[database][collection]
    log.debug(u"向MongoDB的%s.%s插入数据：%s" % (database,collection,data))
    log.info(u"向MongoDB的%s.%s插入数据%s行" % (database,collection,data.shape[0]))
    if data.size != 0:
        return coll.insert_many(data.to_dict(orient='records'))

def update_MongoDB(dbname, database, collection, *args):
    __write_permission(dbname)
    coll = __get_DBconnector(dbname).get_engine()[database][collection]
    log.info(u"从MongoDB的%s.%s修改数据：%s" % (database,collection,str(args)))
    return coll.update_many(*args)

def delete_from_MongoDB(dbname, database, collection, *args):
    __write_permission(dbname)
    coll = __get_DBconnector(dbname).get_engine()[database][collection]
    log.info(u"从MongoDB的%s.%s删除数据：%s" % (database,collection,str(args)))
    return coll.delete_many(*args)

def drop_coll_from_MongoDB(dbname, database, collection, *args):
    __write_permission(dbname)
    coll = __get_DBconnector(dbname).get_engine()[database][collection]
    log.info(u"删除MongoDB的%s.%s：%s" % (database,collection,str(args)))
    return coll.drop(*args)

def create_index_on_MongoDB(dbname, database, collection, *args):
    return __get_DBconnector(dbname).get_engine()[database][collection].create_index(*args)

def get_MongoDB_engine(dbname):
    return __get_DBconnector(dbname)