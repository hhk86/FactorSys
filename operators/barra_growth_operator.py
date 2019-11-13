import numpy as np
import pandas as pd
from operators.operator import Operator


class BarraGrowthOperator(Operator):
    table = 'fac_barra_growth'
    years = 3

    @classmethod
    def load_data(cls, date, code_list=None):
        income = cls.db.query_MongoDB(cls.dbname,'basicdb','basic_income',
                                      {'trade_date':date,'code':{'$nin':cls.utils.get_suspend_list(date)}},
                                      {'_id':0,'trade_date':1,'code':1,'net_income_snapshots':1,'total_revenue_snapshots':1}).set_index('code')
        to_dict = np.vectorize(lambda x:eval(x.replace('nan','np.nan')))
        income['net_income_snapshots'] = to_dict(income['net_income_snapshots'])
        income['total_revenue_snapshots'] = to_dict(income['total_revenue_snapshots'])
        return income

    @classmethod
    def fit(cls, datas):
        @np.vectorize
        def growth(reports):
            latest_report = max(reports.keys())
            vals = [(cls.years,reports.get(latest_report,np.nan))]
            for year in range(cls.years-1, 0, -1):
                latest_report = cls.utils.nearby_season_month(latest_report,-4)
                t1 = cls.utils.nearby_season_month(latest_report,-4)
                t2 = "%s1231" % t1[:4]
                vals.append((year,reports.get(latest_report,np.nan) + reports.get(t2,np.nan) - reports.get(t1,np.nan)))
            vals = pd.DataFrame(vals,columns=['x','y'])
            if vals.dropna(how='any').shape[0]==cls.years:
                return vals['x'].cov(vals['y'])/vals['x'].var()/np.abs(vals['y'].mean())
        datas['egro'] = growth(datas['net_income_snapshots'])
        datas['sgro'] = growth(datas['total_revenue_snapshots'])
        datas['code'] = datas.index
        return datas[['trade_date','code','egro','sgro']]
