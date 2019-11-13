'''
barra Residual Volatility因子计算程序
'''
import numpy as np
import pandas as pd
from operators.operator import Operator


class BarraResidualVolatilityOperator(Operator):
    table = 'fac_barra_residual_vol'

    @classmethod
    def load_data(cls, date, code_list=None):
        from_date = cls.calendar[cls.calendar <= date].iat[-252]
        date_criteria = "TRADE_DT>='%s' and TRADE_DT<='%s'" % (from_date,date)
        rf = cls.db.query_MongoDB(cls.dbname, 'basicdb', 'basic_risk_free_rate', {'trade_date': {'$gte': from_date, '$lte': date}},{'_id': 0})
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
        datas = datas[2].join(datas[1], how='left').join(datas[0],how='left')
        rf = datas['rf']
        benchmark = datas['benchmark']
        datas = datas.drop(['rf', 'benchmark'], axis=1)
        dastd_weight = pd.Series(cls.utils.exp_weight(42, 252),index=datas.index,name='dastd_weight')
        hsigma_weight = pd.Series(cls.utils.exp_weight(63, 252),index=datas.index,name='hsigma_weight')
        def residual_vol(df_one):
            df_tmp = pd.DataFrame({'stock': df_one, 'benchmark': benchmark,'rf': rf}).dropna(how='any').join(dastd_weight,how='left').join(hsigma_weight,how='left')
            hsigma,dastd,cmra = 3*[np.nan]
            if df_tmp['hsigma_weight'].sum() >= 0.8:
                #计算hsigma
                const, beta = cls.utils.beta_wls((df_tmp['benchmark'] - df_tmp['rf']), (df_tmp['stock'] - df_tmp['rf']), df_tmp['hsigma_weight'])
                df_tmp['et'] = df_tmp['stock'] - beta * (df_tmp['benchmark'] - df_tmp['rf']) - const - df_tmp['rf']
                hsigma = np.sqrt(np.cov(df_tmp['et'], rowvar=False, aweights=df_tmp['hsigma_weight']))
            if df_tmp['dastd_weight'].sum() >= 0.8:
                #计算dastd
                re = df_tmp['stock']-df_tmp['rf']
                dastd = np.sqrt(np.sum(np.power(re-re.mean(), 2) * df_tmp['dastd_weight'])/df_tmp['dastd_weight'].sum())
            if df_tmp.shape[0] >= (252*0.5):
                #计算cmra
                months = pd.DataFrame([np.sum(np.log(1+x)).rename(max(x.index)) for x in map(lambda x: df_tmp.iloc[x-21 if x >= 21 else 0:x][['stock', 'rf']], range(df_tmp.shape[0], 0, -21))])
                months.sort_index(ascending=False, inplace=True)
                months = (months['stock']-months['rf']).cumsum()
                cmra = months.max() - months.min()
            return {'code': df_one.name, 'hsigma': hsigma, 'dastd': dastd, 'cmra': cmra}
        result = pd.DataFrame(list(datas.apply(residual_vol).dropna()))
        result['trade_date'] = max(datas.index)
        return result
