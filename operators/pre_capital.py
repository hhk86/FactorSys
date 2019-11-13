import numpy as np
import datetime
from operators.operator import Operator

class PreCapital(Operator):
    schema = 'basicdb'
    table = 'basic_capital'

    @classmethod
    def init_date_list(cls):
        dates = cls.db.get_MongoDB_engine(cls.dbname).get_engine()[cls.schema][cls.table].distinct('trade_date')
        dates.append('20050101')
        from_date = max(dates)
        now = datetime.datetime.now().strftime("%Y%m%d")
        dates = cls.calendar[(cls.calendar > from_date) & (cls.calendar < now)]
        cls.log.info(u"取MongoDB的%s.%s的最大日期%s作为默认起始日期（包含）" % (cls.schema, cls.table, from_date))
        cls.log.info(u"计算周期为%s至%s" % (dates.min(), dates.max()))
        return iter(dates)

    @classmethod
    def load_data(cls, date, code_list=None):
        prices = cls.db.query_by_SQL('wind',r"select S_INFO_WINDCODE as code, TRADE_DT as trade_date, S_DQ_CLOSE as price from wind.AShareEODPrices where TRADE_DT='%s'" % date)
        prices.set_index('code', inplace=True)
        cap = cls.db.query_by_SQL('wind', r"select WIND_CODE as code,TOT_SHR*10000 as cap,FLOAT_SHR*10000 as float_cap,CHANGE_DT1 from wind.AShareCapitalization where (WIND_CODE,CHANGE_DT) in (select WIND_CODE,max(CHANGE_DT) from wind.AShareCapitalization where CHANGE_DT1<='%s' GROUP BY WIND_CODE) order by WIND_CODE,CHANGE_DT1 desc" % (date))
        cap.drop_duplicates(['code'],inplace=True)
        cap.set_index('code', inplace=True)
        return prices.join(cap, how='inner')

    @classmethod
    def fit(cls, datas):
        datas['tot_mv'] = np.round(datas['price'] * datas['cap'], 6)
        datas['float_mv'] = np.round(datas['price'] * datas['float_cap'], 6)
        datas['code'] = datas.index
        return datas[['code', 'trade_date', 'cap', 'tot_mv', 'float_cap', 'float_mv']]