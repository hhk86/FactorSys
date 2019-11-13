import contextlib
import os
import importlib
from sqlalchemy import create_engine, MetaData
from sqlalchemy.orm import sessionmaker

os.environ['NLS_LANG'] = 'SIMPLIFIED CHINESE_CHINA.UTF8'

class SqlConnector(object):
    '''
    支持mysql、oracle、Db2、sql sever等关系型数据库的接入操作
    '''
    __metadata = None
    __engine = None
    __schema = None
    __session_maker = None

    def __init__(self, params):
        C = params
        url = '{}://{}:{}@{}:{}/{}'.format(
            C.get('database.dbconnector'),
            C.get('database.username'),
            C.get('database.password'),
            C.get('database.host'),
            C.get('database.port'),
            C.get('database.database')
        )
        self.__engine = create_engine(url,
                                      pool_size=C.get('database.pool_size', 10),
                                      pool_recycle=C.get('database.pool_recycle', 3600),
                                      echo=C.get('database.echo', 'False').lower() == 'true')
        self.__session_maker = sessionmaker(bind=self.__engine, expire_on_commit=False)
        self.__metadata = MetaData(self.__engine)
        self.__schema = C.get('database.schema',None)

    @contextlib.contextmanager
    def get_session(self):
        s = self.__session_maker()
        try:
            yield s
            s.commit()
        except Exception as e:
            s.rollback()
            raise e
        finally:
            s.close()

    def get_engine(self):
        return self.__engine

    def get_schema(self):
        return self.__schema

class NoSqlConnector(object):
    '''
    nosql数据库目前只支持MongoDB
    '''
    __engine = None
    def __init__(self, params):
        C = params
        client_class = C.get('database.clientclass')
        mod = importlib.import_module(client_class[:client_class.rindex('.')])
        client = getattr(mod, client_class[client_class.rindex('.') + 1:])
        url = '{}://{}:{}@{}:{}'.format(
            C.get('database.dbconnector'),
            C.get('database.username'),
            C.get('database.password'),
            C.get('database.host'),
            C.get('database.port')
        )
        self.__engine = client(url)

    def get_session(self):
        pass

    def get_engine(self):
        return self.__engine
