from operators.operator import Operator


class BarraLeverageOperator(Operator):
    table = 'fac_barra_leverage'

    @classmethod
    def load_data(cls, date, code_list=None):
        balance = cls.db.query_MongoDB(cls.dbname,'basicdb','basic_balance',
                                       {'trade_date':date,'code':{'$nin':cls.utils.get_suspend_list(date)}},
                                       {'_id':0,'code':1,'trade_date':1,'preferred_stock':1,'longterm_loan':1,'total_liabilities':1,'total_assets':1,'total_equities_exc_min':1}).set_index('code')
        mv = cls.db.query_MongoDB(cls.dbname,'basicdb','basic_capital',
                                       {'trade_date':date},
                                       {'_id':0,'code':1,'tot_mv':1}).set_index('code')
        data = balance.join(mv,how='inner')
        data.rename(columns={'tot_mv':'me','preferred_stock':'pe','longterm_loan':'ld','total_liabilities':'td','total_assets':'ta','total_equities_exc_min':'be'},inplace=True)
        # if code_list is not None:
        #     data = data.loc[filter(lambda x:x in data.index,code_list),:]
        return data

    @classmethod
    def fit(cls, datas):
        datas['dtoa'] = datas['td']/datas['ta']
        datas['mlev'] = (datas['me']+datas['pe']+datas['ld'])/datas['me']
        datas['blev'] = (datas['be']+datas['pe']+datas['ld'])/datas['be']
        datas = datas[['trade_date','dtoa','mlev','blev']].dropna(how='all')
        datas['code'] = datas.index
        return datas

