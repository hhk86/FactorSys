from operators.operator import Operator


class BarraEarningsYieldOperator(Operator):
    table = 'fac_barra_earning_yield'

    @classmethod
    def load_data(cls, date, code_list=None):
        data = cls.db.query_MongoDB(cls.dbname, 'basicdb', 'basic_income',
                                       {'trade_date': date, 'code': {'$nin': cls.utils.get_suspend_list(date)}},
                                       {'_id': 0, 'code': 1,'trade_date':1, 'net_income': 1}).set_index('code')
        data = data.join(cls.db.query_MongoDB(cls.dbname, 'basicdb', 'basic_capital',
                                       {'trade_date': date},
                                       {'_id': 0, 'code': 1, 'tot_mv': 1}).set_index('code'),how='inner')
        data = data.join(cls.db.query_MongoDB(cls.dbname,'basicdb','basic_cashflow',
                                       {'trade_date': date},
                                       {'_id':0,'code':1,'operating_cashflow':1}).set_index('code'),how='inner')
        # if code_list is not None:
        #     data = data.loc[filter(lambda x: x in data.index, code_list), :]
        return data

    @classmethod
    def fit(cls, datas):
        datas['cetop'] = datas['operating_cashflow']/datas['tot_mv']
        datas['etop'] = datas['net_income']/datas['tot_mv']
        datas['code'] = datas.index
        return datas[['trade_date','code','cetop','etop']]
