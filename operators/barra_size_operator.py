'''
barra size因子计算程序
'''
import numpy as np
from operators.operator import Operator


class BarraSizeOperator(Operator):
    table = 'fac_barra_size'

    @classmethod
    def load_data(cls, date, code_list=None):
        data = cls.db.query_MongoDB(cls.dbname, 'basicdb', 'basic_capital',
                                    {'trade_date': date, 'code':{'$nin':cls.utils.get_suspend_list(date)}},
                                    {'code': 1, 'trade_date':1, 'tot_mv':1,'_id':0})
        data.dropna(how='any', inplace=True)
        data.rename(columns={'tot_mv':'lncap'}, inplace=True)
        return data

    @classmethod
    def fit(cls, datas):
        datas['lncap'] = np.log(datas['lncap'])
        return datas


