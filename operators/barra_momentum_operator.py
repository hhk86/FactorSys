'''
barra momentum因子计算程序
'''
import numpy as np
import pandas as pd
from operators.operator import Operator

class BarraMomentumOperator(Operator):
    table = 'fac_barra_momentum'

    @classmethod
    def load_data(cls, date, code_list=None):
        from_date = cls.calendar[cls.calendar <= date].iat[-504-21]
        date_criteria = "TRADE_DT>='%s' and TRADE_DT<='%s'" % (from_date,date)
        rf = cls.db.query_MongoDB(cls.dbname, 'basicdb', 'basic_risk_free_rate', {'trade_date': {'$gte': from_date, '$lte': date}},{'_id': 0})
        data = cls.db.query_by_SQL('wind',r"select S_INFO_WINDCODE as code,TRADE_DT as trade_date,S_DQ_PCTCHANGE/100 as ret FROM wind.AShareEODPrices where %s and S_DQ_VOLUME!=0" % date_criteria)
        rf.set_index('trade_date',inplace=True)
        data = data.pivot(index='trade_date', columns='code', values='ret')
        suspend_list = filter(lambda x: x in data.columns,cls.utils.get_suspend_list(date))
        data.drop(suspend_list, axis=1, inplace=True)
        # if code_list is not None:
        #     data = data[filter(lambda x:x in data.columns,code_list)]
        return rf, data

    @classmethod
    def fit(cls, datas):
        datas = datas[1].join(datas[0], how='left')
        date = max(datas.index)
        datas = datas.iloc[:-21]
        rf = datas['rf']
        datas = datas.drop(['rf'], axis=1)
        weight = pd.Series(cls.utils.exp_weight(126, 504),index=datas.index,name='weight')
        def momentum(df_one):
            df_tmp = pd.DataFrame({'stock': df_one, 'rf': rf}).dropna(how='any').join(weight,how='left')
            if df_tmp['weight'].sum() >= 0.8:
                df_tmp[['stock','rf']] = np.log(df_tmp[['stock','rf']]+1)
                return np.sum((df_tmp['stock']-df_tmp['rf']) * df_tmp['weight'])/df_tmp['weight'].sum()
        result = datas.apply(momentum).dropna().rename('rstr').to_frame().reset_index().rename(columns={'index':'code'})
        result['trade_date'] = date
        return result
