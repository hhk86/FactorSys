from common.logger import log,default_logger_decorator, operator_logger_decorator
from common.sys_config import conf_dict
from common import db

#默认加载交易日历
calendar = db.query_by_SQL('wind',r"select TRADE_DAYS as trade_date from wind.AShareCalendar where S_INFO_EXCHMARKET='SZSE' and TRADE_DAYS>='20000101' order by TRADE_DAYS")['trade_date']
