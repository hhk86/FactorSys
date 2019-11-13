import numpy as np
import pandas as pd
from operators.operator import Operator


class PreRiskFreeRate(Operator):
    schema = 'basicdb'
    table = 'basic_risk_free_rate'

    @classmethod
    def init_date_list(cls):
        import datetime
        now = datetime.datetime.now().strftime("%Y%m%d")
        return [now]

    @classmethod
    def load_data(cls, date, code_list=None):
        rf = cls.db.query_by_SQL('wind',r"SELECT TRADE_DT as trade_date,B_ANAL_YIELD/100/252 as rf FROM wind.CBONDCURVECSI WHERE B_ANAL_CURVENAME = '国债收益率曲线' AND B_ANAL_CURVETERM = '1' AND B_ANAL_CURVETYPE = '1' order by TRADE_DT").set_index('trade_date')
        rf = rf.join(pd.DataFrame(index=cls.calendar[cls.calendar<=date]),how='outer')
        cls.log.info(u"默认从%s至%s全量刷新。" % (min(rf.index), max(rf.index)))
        return rf

    @classmethod
    def dump_data(cls, datas):
        from_date = datas['trade_date'].min()
        to_date = datas['trade_date'].max()
        delete_criteria = {'trade_date': {'$gte': from_date, '$lte': to_date}}
        cls.db.delete_from_MongoDB(cls.dbname, cls.schema, cls.table, delete_criteria)
        cls.db.insert_to_MongoDB(cls.dbname, cls.schema, cls.table, datas)

    @classmethod
    def fit(cls, datas):
        datas['rf'] = datas['rf'].fillna(method='ffill')
        datas['rf'] = datas['rf'].fillna(method='bfill')
        datas['rf'] = np.round(datas['rf'], 6)
        datas['trade_date'] = datas.index
        return datas