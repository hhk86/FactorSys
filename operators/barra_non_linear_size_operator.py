'''
barra non linear size 因子计算程序
'''
import numpy as np
from operators.operator import Operator

class BarraNonLinearSizeOperator(Operator):
    table = 'fac_barra_non_linear_size'

    @classmethod
    def load_data(cls, date, code_list=None):
        data = cls.db.query_MongoDB(cls.dbname, 'basicdb', 'basic_capital',
                                    {'trade_date': date, 'code':{'$nin':cls.utils.get_suspend_list(date)}},
                                    {'code': 1, 'trade_date':1, 'tot_mv':1,'_id':0})
        data.dropna(how='any', inplace=True)
        data.rename(columns={'tot_mv':'val'}, inplace=True)
        return data

    @classmethod
    def fit(cls, datas):
        datas['weight'] = 1#等权
        datas['lncap'] = np.log(datas['val'])
        datas.dropna(how='any', inplace=True)
        datas['nlsize'] = np.power(datas['lncap'], 3)
        const, beta = cls.utils.beta_wls(datas['lncap'], datas['nlsize'], datas['weight'])
        datas['nlsize'] = (datas['nlsize'] - beta * datas['lncap'] - const)
        return datas[['code','trade_date','nlsize']]


