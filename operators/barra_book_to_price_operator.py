'''
barra bp因子计算程序
'''
import numpy as np
from operators.operator import Operator


class BarraBookToPriceOperator(Operator):
    table = 'fac_barra_book_to_price'

    @classmethod
    def load_data(cls, date, code_list=None):
        data = cls.db.query_MongoDB(cls.dbname, 'basicdb', 'basic_capital',
                                    {'trade_date': date, 'code':{'$nin':cls.utils.get_suspend_list(date)}},
                                    {'code': 1, 'trade_date':1, 'tot_mv':1,'_id':0}).set_index('code')
        data = data.join(cls.db.query_MongoDB(cls.dbname,'basicdb','basic_balance',
                                  {'trade_date': date},
                                  {'code': 1,'total_equities_exc_min': 1, '_id': 0}).set_index('code'),how='inner')
        data.dropna(how='any', inplace=True)
        return data

    @classmethod
    def fit(cls, datas):
        datas['btop'] = datas['total_equities_exc_min']/datas['tot_mv']
        datas['code'] = datas.index
        return datas[['code','trade_date','btop']]




