'''
barra liquidity因子计算程序
'''
import numpy as np
import pandas as pd
from scipy.stats import trim_mean
from operators.operator import Operator


class BarraLiquidityOperator(Operator):
    table = 'fac_barra_liquidity'

    @classmethod
    def load_data(cls, date, code_list=None):
        from_date = cls.calendar[cls.calendar <= date].iat[-252]
        date_criteria = "TRADE_DT>='%s' and TRADE_DT<='%s'" % (from_date,date)
        volume = cls.db.query_by_SQL('wind',r"select S_INFO_WINDCODE as code,TRADE_DT as trade_date,S_DQ_VOLUME*100 as volume FROM wind.AShareEODPrices where %s" % date_criteria)
        volume = volume.pivot(index='trade_date',columns='code',values='volume')
        cap = cls.db.query_MongoDB(cls.dbname, 'basicdb', 'basic_capital',
                                    {'trade_date':  {'$gte': from_date, '$lte': date}},
                                    {'code': 1, 'trade_date':1, 'float_cap':1,'_id':0})
        cap = cap.pivot(index='trade_date', columns='code', values='float_cap')
        index = set(volume.index).intersection(cap.index)
        columns = set(volume.columns).intersection(cap.columns).difference(set(cls.utils.get_suspend_list(date)))
        # if code_list is not None:
        #     columns = set(filter(lambda x:x in columns,code_list))
        columns = sorted(list(columns),reverse=False)
        index = sorted(list(index),reverse=False)
        volume = volume.loc[index,columns]
        cap = cap.loc[index,columns]
        return volume/cap

    @classmethod
    def fit(cls, datas):
        def liquidity(df_one):
            stom, stoq, stoa = [None] * 3
            if df_one[-21:][df_one>0].size>=(21*0.5):
                stom = np.log(trim_mean(df_one[-21:][df_one>0],0.1))
            if df_one[-63:][df_one>0].size>=(63*0.5):
                stoq = np.log(trim_mean(df_one[-63:][df_one>0],0.1))
            if df_one[df_one>0].size>=(252*0.5):
                stoa = np.log(trim_mean(df_one[df_one>0],0.1))
            return {'code': df_one.name, 'stom': stom, 'stoq': stoq, 'stoa': stoa}
        result = pd.DataFrame(list(datas.apply(liquidity).dropna()))
        result['trade_date'] = max(datas.index)
        return result

