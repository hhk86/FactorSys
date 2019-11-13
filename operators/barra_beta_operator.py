'''
barra beta因子计算程序
'''
import pandas as pd
from operators.operator import Operator

class BarraBetaOperator(Operator):
    table = 'fac_barra_beta'

    @classmethod
    def load_data(cls, date, code_list=None):
        from_date = cls.calendar[cls.calendar <= date].iat[-252]
        date_criteria = "TRADE_DT>='%s' and TRADE_DT<='%s'" % (from_date,date)
        rf = cls.db.query_MongoDB(cls.dbname, 'basicdb', 'basic_risk_free_rate', {'trade_date': {'$gte': from_date, '$lte': date}}, {'_id': 0})
        benchmark = cls.db.query_by_SQL('wind',r"select TRADE_DT as trade_date,S_DQ_PCTCHANGE/100 as benchmark from wind.AIndexEODPrices where S_INFO_WINDCODE = '000985.CSI' and %s order by TRADE_DT" % (date_criteria))
        data = cls.db.query_by_SQL('wind',r"select S_INFO_WINDCODE as code,TRADE_DT as trade_date,S_DQ_PCTCHANGE/100 as ret FROM wind.AShareEODPrices where %s and S_DQ_VOLUME!=0" % date_criteria)
        rf.set_index('trade_date',inplace=True)
        benchmark.set_index('trade_date', inplace=True)
        data = data.pivot(index='trade_date', columns='code', values='ret')
        suspend_list = filter(lambda x: x in data.columns,cls.utils.get_suspend_list(date))
        data.drop(suspend_list, axis=1, inplace=True)
        # if code_list is not None:
        #     data = data[filter(lambda x:x in data.columns,code_list)]
        return rf, benchmark, data

    @classmethod
    def fit(cls, datas):
        datas = datas[2].join(datas[1], how='left').join(datas[0], how='left')
        rf = datas['rf']
        benchmark = datas['benchmark'] - rf
        datas = datas.drop(['rf','benchmark'], axis=1).sub(rf, axis=0)
        weight = pd.Series(cls.utils.exp_weight(63, 252),index=datas.index,name='weight')
        def beta(df_one):
            df_tmp = pd.DataFrame({'y': df_one, 'x': benchmark}).dropna(how='any').join(weight,how='left')
            if df_tmp['weight'].sum() >= 0.8:
                return cls.utils.beta_wls(df_tmp['x'], df_tmp['y'], df_tmp['weight'])[1]
        result = datas.apply(beta).dropna().rename('beta').to_frame().reset_index().rename(columns={'index':'code'})
        result['trade_date'] = max(datas.index)
        return result
