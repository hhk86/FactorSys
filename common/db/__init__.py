'''
通用数据库操作包，支持主流sql数据库和MongoDB
#引入
import db
#sql类数据操作
dataFrame = db.getDataBySQL('wind',r"select * from tablename")
#Nosql类数据库操作（仅支持MongoDB）
dataFrame = db.queryFromMongoDB('mongodb', 'database', 'collection', {}, {'_id': 0})
db.insertToMongoDB('mongodb', 'database', 'collection',dataFrame)
'''
from common.db import __core

def query_by_SQL(dbname, sql):
    return __core.query(dbname, sql)

def update_by_SQL(dbname, sql):
    return __core.update(dbname, sql)

def insert_by_SQL(dbname, tableName, data, append=True):
    if append:
        __core.insert(dbname, tableName, data)
    else:
        __core.truncate(dbname, tableName)
        __core.insert(dbname, tableName, data)

def delete_by_SQL(dbname, sql):
    __core.delete_data(dbname, sql)

def query_MongoDB(dbname, database, collection, *args):
    return __core.query_MongoDB(dbname, database, collection, *args)

def insert_to_MongoDB(dbname, database, collection, data):
    return __core.insert_to_MongoDB(dbname, database, collection, data)

def update_MongoDB(dbname, database, collection, *args):
    return __core.update_MongoDB(dbname, database, collection, *args)

def delete_from_MongoDB(dbname, database, collection, *args):
    return __core.delete_from_MongoDB(dbname, database, collection, *args)

def drop_coll_from_MongoDB(dbname, database, collection, *args):
    return __core.drop_coll_from_MongoDB(dbname, database, collection, *args)

def create_index_on_MongoDB(dbname, database, collection, *args):
    return __core.create_index_on_MongoDB(dbname, database, collection, *args)

def get_MongoDB_engine(dbname):
    return __core.get_MongoDB_engine(dbname)