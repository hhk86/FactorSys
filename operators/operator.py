'''
因子计算程序抽象父类
'''
import abc
import utils
import datetime
from common import db, log, operator_logger_decorator, conf_dict, calendar

class Operator(metaclass=abc.ABCMeta):
    db = db
    log = log
    decorator = operator_logger_decorator
    utils = utils
    conf_dict = conf_dict
    calendar = calendar
    dbname = 'factor'
    schema = 'factordb'

    def __init__(self):
        raise Exception(u'operator无需实例化')

    @classmethod
    def init_date_list(cls):
        '''
        返回需计算因子的日期序列。
        截面计算的因子无需重写该方法。
        但是，例如size因子，计算逻辑跟时序和截面均无关，需重写该方法。重写方式见barra_size_operator
        :return: iteractor  需计算因子的日期序列
        '''
        dates = cls.db.get_MongoDB_engine(cls.dbname).get_engine()[cls.schema][cls.table].distinct('trade_date')
        dates.append(conf_dict.get('sys.fromdate'))
        from_date = max(dates)
        now = datetime.datetime.now().strftime("%Y%m%d")
        dates = cls.calendar[(cls.calendar > from_date) & (cls.calendar < now)]
        log.info(u"取MongoDB的%s.%s的最大日期%s作为默认起始日期（包含）" % (cls.schema, cls.table, from_date))
        log.info(u"计算周期为%s至%s" % (dates.min(), dates.max()))
        return iter(dates)

    @classmethod
    @abc.abstractmethod
    def load_data(cls, date, code_list=None):
        '''
        数据导入函数，抽象方法，需子类实现
        :param date: string，计算date期间的因子数据
        :param code_list: list，默认为None即计算全部A股股票，待计算因子的股票代码列表
        :return:加载数据结果，返回结构需与fit函数的处理逻辑对应
        '''
        pass

    @classmethod
    @abc.abstractmethod
    def fit(cls, datas):
        '''
        数据计算函数，抽象方法，需子类实现
        :param datas: 任意类型形式，入参的处理逻辑需与load_data的返回结果对应
        :return:计算的结果数据，返回结构需与dump_data的处理结构对应
        '''
        pass

    @classmethod
    def dump_data(cls, datas):
        '''
        数据存储默认方法，如有不同存储方式，子类可重写该方法。
        :param datas: dataframe 待存储数据，必须有trade_date字段，且不为空
        :return:
        '''
        date = datas['trade_date'].max()
        delete_criteria = {'trade_date': date}
        cls.db.delete_from_MongoDB(cls.dbname, cls.schema, cls.table, delete_criteria)
        cls.db.insert_to_MongoDB(cls.dbname, cls.schema, cls.table, datas)

    @classmethod
    @operator_logger_decorator
    def execute(cls, date, code_list=None):
        '''
        operator统一入口函数，子类无需实现
        :param date: string，计算date期间的因子数据
        :param code_list: list，待计算因子的股票代码列表
        :return:无返回
        '''
        return cls.dump_data(cls.fit(cls.load_data(date, code_list)))
