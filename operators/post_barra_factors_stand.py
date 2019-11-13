'''
barra因子去极值及标准化处理函数
'''
import pandas as pd
import statsmodels.api as sm
from operators.operator import Operator


class PostBarraFactorsStand(Operator):
    table = 'fac_stand_factors'
    tables = ['fac_barra_beta',
              'fac_barra_momentum',
              'fac_barra_non_linear_size',
              'fac_barra_residual_vol',
              'fac_barra_book_to_price',
              'fac_barra_liquidity',
              'fac_barra_earning_yield',
              'fac_barra_leverage',
              'fac_barra_growth',
              'fac_barra_size']

    @classmethod
    def load_data(cls, date, code_list=None):
        data = pd.DataFrame()
        for tab in cls.tables:
            fac = cls.db.query_MongoDB(cls.dbname, cls.schema, tab,
                                    {'trade_date': date, 'code':{'$nin':cls.utils.get_suspend_list(date)}},
                                    {'_id':0,'trade_date':0})
            data = data.join(fac.set_index('code'),how='outer') if fac.shape[0]!=0 else data
        undrop_shape = data.shape[0]
        data.dropna(how='any',inplace=True)
        droped_shape = data.shape[0]
        assert droped_shape >= (0.7*undrop_shape)
        return date, data

    @classmethod
    def fit(cls, datas):
        date, datas = datas
        #去极值
        datas = datas.apply(cls.utils.median_absolute_deviation)
        #标准化
        datas = datas.apply(cls.utils.z_score)
        #二级因子归并为一级因子
        datas.rename(columns={'rstr':'momentum','lncap':'size'},inplace=True)
        datas['residual_vol'] = cls.utils.z_score(0.74 * datas['dastd'] + 0.16 * datas['cmra'] + 0.1 * datas['hsigma'])
        params = sm.OLS(datas['residual_vol'], datas[['beta','size']]).fit().params[['beta','size']]
        datas['residual_vol'] = cls.utils.z_score(datas['residual_vol'] - (params * datas[['beta','size']]).sum(axis=1))
        datas['earnings_yield'] = cls.utils.z_score(0.5 * datas['etop'] + 0.5 * datas['cetop'])
        datas['growth'] = cls.utils.z_score(0.5 * datas['sgro'] + 0.5 * datas['egro'])
        datas['leverage'] = cls.utils.z_score(0.38 * datas['mlev'] + 0.35 * datas['dtoa'] + 0.27 * datas['blev'])
        datas['liquidity'] = cls.utils.z_score(0.35 * datas['stom'] + 0.35 * datas['stoq'] + 0.30 * datas['stoa'])
        datas['trade_date'] = date
        datas['code'] = datas.index
        return datas[['code','trade_date','size','nlsize','beta','momentum','residual_vol','btop','earnings_yield','growth','leverage','liquidity']]


