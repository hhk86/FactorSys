import datetime
import numpy as np
import pandas as pd
from models.model import Model


class BarraCNE5RetModel(Model):
    table = 'fac_barra_ret'

    #中信行业
    industries = ["医药",
                  "纺织服装",
                  "房地产",
                  "电力及公用事业",
                  "机械",
                  "建筑",
                  "有色金属",
                  "商贸零售",
                  "通信",
                  "交通运输",
                  "电子元器件",
                  "汽车",
                  "食品饮料",
                  "基础化工",
                  "电力设备",
                  "农林牧渔",
                  "石油石化",
                  "煤炭",
                  "计算机",
                  "轻工制造",
                  "餐饮旅游",
                  "钢铁",
                  "综合",
                  "传媒",
                  "建材",
                  "银行",
                  "证券Ⅱ","信托及其他","保险Ⅱ",
                  "家电",
                  "国防军工"]
    droped_industry = '汽车'

    factors = None
    industries_weight = None
    weight = None
    R = None

    @classmethod
    def init_date_list(cls):
        '''
        返回需计算的日期序列。
        :return: iteractor  需计算的日期序列
        '''
        dates = cls.db.get_MongoDB_engine(cls.dbname).get_engine()[cls.schema][cls.table].distinct('trade_date')
        sys_date = cls.calendar[cls.calendar > cls.conf_dict.get('sys.fromdate')].iat[0]
        dates.append(sys_date)
        from_date = max(dates)
        now = datetime.datetime.now().strftime("%Y%m%d")
        dates = cls.calendar[(cls.calendar > from_date) & (cls.calendar < now)]
        cls.log.info(u"取MongoDB的%s.%s的最大日期%s作为默认起始日期（包含）" % (cls.schema, cls.table, from_date))
        cls.log.info(u"计算周期为%s至%s" % (dates.min(), dates.max()))
        return iter(dates)

    @classmethod
    def cache(cls, date, code_list=None):
        #获取上一日的因子暴露度、行业哑变量、行业权重等数据
        date_shift1 = cls.calendar[cls.calendar < date].iat[-1]

        #行业哑变量
        industries_dummy_variable = cls.db.query_MongoDB(cls.dbname, 'universedb', 'universe',
                                                         {'trade_date': date_shift1, 'citic_industry_L1': {'$in': cls.industries}},
                                                         {'_id': 0, 'code': 1, 'citic_industry_L1': 1}).rename(columns={'citic_industry_L1': 'industry'})
        industries_dummy_variable = industries_dummy_variable.append(cls.db.query_MongoDB('factor', 'universedb', 'universe',
                                            {'trade_date': date_shift1, 'citic_industry_L2': {'$in': cls.industries}},
                                            {'_id': 0, 'code': 1, 'citic_industry_L2': 1}).rename(columns={'citic_industry_L2': 'industry'}))
        industries_dummy_variable['val'] = 1
        industries_dummy_variable = industries_dummy_variable.pivot(index='code', columns='industry', values='val').fillna(0)

        cls.weight = cls.db.query_MongoDB(cls.dbname, 'basicdb', 'basic_capital', {'trade_date': date_shift1}, {'_id': 0, 'code': 1, 'float_mv': 1}).set_index('code')
        #行业权重
        industries_dummy_variable = industries_dummy_variable.join(cls.weight, how='inner')
        cls.industries_weight = industries_dummy_variable['float_mv']
        industries_dummy_variable.drop(['float_mv'], axis=1, inplace=True)
        cls.industries_weight = industries_dummy_variable.mul(cls.industries_weight, axis=0).sum()
        cls.industries_weight = cls.industries_weight / cls.industries_weight.sum()
        cls.industries_weight = -(cls.industries_weight / cls.industries_weight[cls.droped_industry]).drop(cls.droped_industry)
        #股票权重
        cls.weight['weight'] = 1.0/np.sqrt(cls.weight)
        cls.weight = cls.weight[['weight']]

        #因子暴露度
        cls.factors = cls.db.query_MongoDB(cls.dbname, cls.schema,'fac_stand_factors', {'trade_date':date_shift1}, {'_id':0, 'trade_date':0}).set_index('code')
        cls.factors = industries_dummy_variable.join(cls.factors, how='inner')
        cls.factors['const'] = 1

        #约束矩阵
        cls.R = pd.DataFrame(np.eye(len(cls.factors.columns)), index=cls.factors.columns.tolist(), columns=cls.factors.columns.tolist())
        cls.R.drop(cls.droped_industry, axis=1, inplace=True)
        cls.R.loc[cls.droped_industry, cls.industries_weight.index] = cls.industries_weight

    @classmethod
    def load_data(cls, date, code_list=None):
        cls.cache(date)
        rets = cls.db.query_by_SQL('wind',r"select S_INFO_WINDCODE as code,S_DQ_PCTCHANGE/100 as y from wind.AShareEODPrices where TRADE_DT='%s'" %
                                   date).set_index('code')['y']
        if code_list!=None:
            rets = rets[filter(lambda x:x in rets.columns, code_list)]
        return rets.rename(date)

    @classmethod
    def fit(cls, datas):
        x = cls.factors.join(datas.rename('y'), how='inner').join(cls.weight, how='inner')
        error_industry = x.iloc[:, :31].sum() < 0.5
        error_industry = error_industry[error_industry].index.values
        x.drop(error_industry, axis=1, inplace=True)
        cls.log.error("%s行业的可计算公司数为零，导致X矩阵不可逆。已从X矩阵中删除该行业以保证后续计算。" % error_industry)
        y = x['y']
        w = x['weight']
        x.drop(['y', 'weight'], axis=1, inplace=True)
        r_star = cls.utils.wls(np.dot(x, cls.R), y.values, w.values)
        ret = pd.Series(np.dot(cls.R, r_star), index=x.columns, name=datas.name)
        res = (y - np.dot(x,ret)).rename(datas.name)
        ret = ret.drop(cls.industries).drop('const')
        return res,ret

    @classmethod
    def dump_data(cls, datas):
        res,ret = datas
        ret['trade_date'] = ret.name
        delete_criteria = {'trade_date': ret.name}
        ret = ret.to_frame().T
        cls.db.delete_from_MongoDB(cls.dbname, cls.schema, cls.table, delete_criteria)
        cls.db.insert_to_MongoDB(cls.dbname, cls.schema, cls.table, ret)
        res['trade_date'] = res.name
        delete_criteria = {'trade_date': res.name}
        res = res.to_frame().T
        res.columns = map(lambda x:x.replace('.',''),res.columns)
        cls.db.delete_from_MongoDB(cls.dbname, cls.schema, 'fac_barra_res', delete_criteria)
        cls.db.insert_to_MongoDB(cls.dbname, cls.schema, 'fac_barra_res', res)
