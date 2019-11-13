import numpy as np
import pandas as pd
from models.model import Model
import datetime
import statsmodels.api as sm

class BarraCNE5RiskModel(Model):
    table = 'fac_barra_risk'
    industry = ['CI005001.WI',
                'CI005002.WI',
                'CI005003.WI',
                'CI005004.WI',
                'CI005005.WI',
                'CI005006.WI',
                'CI005007.WI',
                'CI005008.WI',
                'CI005009.WI',
                'CI005010.WI',
                'CI005011.WI',
                'CI005012.WI',
                'CI005013.WI',
                'CI005014.WI',
                'CI005015.WI',
                'CI005016.WI',
                'CI005017.WI',
                'CI005018.WI',
                'CI005019.WI',
                'CI005020.WI',
                'CI005021.WI',
                'CI005165.WI','CI005166.WI','CI005167.WI',
                'CI005023.WI',
                'CI005024.WI',
                'CI005025.WI',
                'CI005026.WI',
                'CI005027.WI',
                'CI005028.WI',
                'CI005029.WI']
    fvra = None
    dvra = None
    factors = None
    @classmethod
    def init_date_list(cls):
        '''
        返回需计算的日期序列。
        :return: iteractor  需计算的日期序列
        '''
        dates = cls.db.get_MongoDB_engine(cls.dbname).get_engine()[cls.schema][cls.table].distinct('trade_date')
        sys_date = cls.calendar[cls.calendar > cls.conf_dict.get('sys.fromdate')].iat[253]
        dates.append(sys_date)
        from_date = max(dates)
        now = datetime.datetime.now().strftime("%Y%m%d")
        dates = cls.calendar[(cls.calendar > from_date) & (cls.calendar < now)]
        cls.log.info(u"取MongoDB的%s.%s的最大日期%s作为默认起始日期（包含）" % (cls.schema, cls.table, from_date))
        cls.log.info(u"计算周期为%s至%s" % (dates.min(), dates.max()))
        return iter(dates)

    @classmethod
    def cache(cls, date, code_list=None):
        date_shift1 = cls.calendar[cls.calendar < date].iat[-1]
        from_date = cls.calendar[cls.calendar <= date_shift1].iat[-252]
        factor_rets = cls.db.query_by_SQL('wind',r"select S_INFO_WINDCODE as code,TRADE_DT as trade_date,S_DQ_PCTCHANGE/100 as ret from wind.AIndexIndustriesEODCITICS where TRADE_DT>='%s' and TRADE_DT<='%s' and S_INFO_WINDCODE in %s" % (from_date,date_shift1,cls.utils.list_convert_tuple_str(cls.industry)))
        factor_rets = factor_rets.pivot(index='trade_date',columns='code',values='ret')
        factor_rets = factor_rets.join(cls.db.query_MongoDB('factor','factordb','fac_barra_ret',{'trade_date':{'$gte': from_date, '$lte': date_shift1}},{'_id':0}).set_index('trade_date'))
        factor_rets = factor_rets.join(cls.db.query_by_SQL('wind',r"select TRADE_DT as trade_date,S_DQ_PCTCHANGE/100 as benchmark from wind.AIndexEODPrices where S_INFO_WINDCODE = '000985.CSI' and TRADE_DT>='%s' and TRADE_DT<='%s'" % (from_date,date_shift1)).set_index('trade_date'))
        stock_res = cls.db.query_MongoDB('factor','factordb','fac_barra_res',{'trade_date':{'$gte': from_date, '$lte': date_shift1}},{'_id':0}).set_index('trade_date')
        stock_res.columns = map(lambda x:"%s.%s" % (x[:6],x[6:]),stock_res.columns)
        fnw = cls.newey_west_adj(factor_rets,2,90,21)
        feig = cls.eig_adj(fnw,10000,1.2)
        cls.fvra = cls.vol_regime_adj(factor_rets,feig,252,42)

        dnw = pd.Series(cls.newey_west_adj(stock_res,5,90,21,True).diagonal(),index=stock_res.columns,name='dnw')
        dtheta = cls.structural_adj(stock_res,dnw,date_shift1)
        tot_mv = cls.db.query_MongoDB('factor','basicdb','basic_capital',
                                      {'trade_date':{'$gte': from_date, '$lte': date_shift1}},
                                      {'_id':0,'code':1,'trade_date':1,'tot_mv':1})
        tot_mv = tot_mv.pivot(index='trade_date',columns='code',values='tot_mv')
        dsh = cls.bayesian_adj(dtheta,tot_mv.loc[date_shift1].dropna().sort_values())
        cls.dvra = cls.vol_regime_adj_res(stock_res,dsh,tot_mv,252,42)

    @classmethod
    def load_data(cls, date, code_list=None):
        if code_list is None or (not isinstance(code_list, pd.Series)):
            raise ValueError('必须输入组合配置信息')
        cls.cache(date)
        codes = list(filter(lambda x:x in cls.factors.index,code_list.index))
        if len(codes)<code_list.size:
            raise ValueError('存在股票无法获取因子数据：%s' % str(set(code_list.index)-set(codes)))
        weight = code_list[codes]
        return codes,weight/np.sum(weight)

    @classmethod
    def fit(cls, datas):
        codes,weight = datas
        X = cls.factors.loc[codes].values
        w = weight.values
        V = np.dot(np.dot(X,cls.fvra),X.T)+np.diag(cls.dvra[codes])
        risk = w.T.dot(V).dot(w)
        return risk

    @classmethod
    def dump_data(cls, datas):
        return datas

    @classmethod
    def newey_west_adj(cls, rets, D, half_life, prediction_period,only_var=False):
        # Newey-West自相关调整
        fraw = cls.utils.exp_weight_cov(rets.values,half_life,diagonal=only_var)
        c_delta_sum = 0
        for d in range(1, D + 1):
            delta = rets.iloc[d:].join(rets.shift(d).iloc[d:], rsuffix='_r')
            c_delta = cls.utils.exp_weight_cov(delta.values,half_life,diagonal=only_var)[:rets.shape[1], -rets.shape[1]:]
            c_delta_sum = c_delta_sum + (1 - d / (D + 1)) * (c_delta + c_delta.T)
        return prediction_period * (fraw + c_delta_sum)

    @classmethod
    def eig_adj(cls,df,M,adjust):
        #特征值调整
        e_value,e_vector = np.linalg.eig(df)
        def eig_risk_adjust():
            bm = list()
            for v in e_value:
                bm.append(np.random.normal(0,np.sqrt(v),252))
            bm = np.array(bm)
            rm = np.dot(e_vector,bm)
            fmc = np.cov(rm)
            val,vec = np.linalg.eig(fmc)
            val_ = np.dot(np.dot(vec.T, df), vec).diagonal()
            return val_/val
        lamda = pd.DataFrame(list(map(lambda x:eig_risk_adjust(),range(M))))
        lamda = np.sqrt(np.mean(lamda))
        gama = (lamda-1)*adjust+1
        e_value_ = np.diag(np.square(gama)*e_value)
        return np.dot(np.dot(e_vector,e_value_),e_vector.T)

    @classmethod
    def vol_regime_adj(cls, rets, df,T,half_life):
        #波动率偏误调整
        bf = np.mean(np.square(rets / np.std(rets)), axis=1)
        wei = cls.utils.exp_weight(half_life,T)
        lamda = np.sum(bf * wei)
        return lamda * df

    @classmethod
    def vol_regime_adj_res(cls, res, dsh, mv, T, half_life):
        #波动率偏误调整
        mv_weight = mv[res.columns]
        bf = np.square(res / np.std(res))
        bf.replace(np.inf,np.nan,inplace=True)
        mv_weight[bf.isna()] = np.nan
        mv_weight = mv_weight.div(mv_weight.sum(axis=1),axis=0)
        bf = np.sum(bf * mv_weight,axis=1)
        wei = cls.utils.exp_weight(half_life,T)
        lamda = np.sum(bf * wei)
        return np.sqrt(lamda) * dsh

    @classmethod
    def structural_adj(cls,stock_res,dnw,date_shift1):
        def structural(rets):
            tmp = rets[~np.isnan(rets)]
            q1,q3 = np.percentile(tmp, [25, 75])
            sigma_ = (1/1.35)*(q3-q1)
            zu = np.abs((tmp.std()-sigma_)/sigma_)
            return np.min([1,np.max([0,(tmp.size-60)/120])])*np.min([1,np.max([0,np.exp(1-zu)])])
        gamas = stock_res.apply(structural)
        Y = np.log(dnw[gamas==1])
        X = cls.db.query_by_SQL('wind',"select S_INFO_WINDCODE as industry, S_CON_WINDCODE as code from wind.AIndexMembersCITICS where S_INFO_WINDCODE in %s and S_CON_INDATE<='%s' and (S_CON_OUTDATE>'%s' or S_CON_OUTDATE is NULL)" % (cls.utils.list_convert_tuple_str(cls.industry),date_shift1,date_shift1))
        X = X.append(cls.db.query_by_SQL('wind',"select S_INFO_WINDCODE as industry, S_CON_WINDCODE as code from wind.AIndexMembersCITICS2 where S_INFO_WINDCODE in %s and S_CON_INDATE<='%s' and (S_CON_OUTDATE>'%s' or S_CON_OUTDATE is NULL)" % (cls.utils.list_convert_tuple_str(cls.industry), date_shift1, date_shift1)))
        X['val'] = 1
        X = X.pivot(index='code', columns='industry',values='val').fillna(0)
        weight = cls.db.query_MongoDB(cls.dbname, 'basicdb', 'basic_capital', {'trade_date': date_shift1},{'_id': 0, 'code': 1, 'float_mv': 1}).set_index('code')
        # 行业权重
        X = X.join(weight, how='inner')
        industries_weight = X['float_mv']
        X.drop(['float_mv'], axis=1, inplace=True)
        industries_weight = X.mul(industries_weight, axis=0).sum()
        industries_weight = industries_weight / industries_weight.sum()
        industries_weight = -(industries_weight / industries_weight['CI005013.WI']).drop('CI005013.WI')
        # 股票权重
        weight['weight'] = 1.0 / np.sqrt(weight)
        weight = weight[['weight']]
        X = X.join(cls.db.query_MongoDB(cls.dbname, cls.schema,'fac_stand_factors', {'trade_date':date_shift1}, {'_id':0, 'trade_date':0}).set_index('code'),how='inner')
        X['const'] = 1
        cls.factors = X.copy()
        R = pd.DataFrame(np.eye(len(X.columns)), index=X.columns.tolist(), columns=X.columns.tolist())
        R.drop('CI005013.WI', axis=1, inplace=True)
        R.loc['CI005013.WI', industries_weight.index] = industries_weight
        X_gama_1 = X.join(Y.rename('y'), how='inner').join(weight, how='inner')
        Y = X_gama_1['y']
        weight = X_gama_1['weight']
        X_gama_1.drop(['y', 'weight'], axis=1, inplace=True)
        r_star = sm.WLS(Y,np.dot(X_gama_1, R), weight).fit().params
        b = pd.Series(np.dot(R, r_star), index=X_gama_1.columns, name=Y.name)
        std = pd.Series(1.05*np.exp(np.dot(X,b)),index=X.index,name='std')
        std = dnw.to_frame().join(std,how='left').fillna(method='ffill',axis=1).join(gamas.rename('gama'),how='left')
        std = std['dnw']*std['gama']+std['std']*(1-std['gama'])
        return std.rename('sigma')

    @classmethod
    def bayesian_adj(cls,dtheta,tot_mv):
        n=10
        size,mod = np.divmod(tot_mv.size,n)
        dsh = pd.Series()
        for group in [tot_mv.iloc[x-size if x-size>mod else 0:x] for x in range(size+mod,tot_mv.size+1,size)]:
            group = group.to_frame('tot_mv').join(dtheta.rename('sigma'),how='left')
            group['tot_mv'] = group['tot_mv']/group.dropna(how='any')['tot_mv'].sum()
            sigma_mean = (group.ix[0]*group.ix[1]).sum()
            delta_ = np.sqrt(np.mean(np.square(group['sigma']-sigma_mean)))
            sub_sigma = np.abs(group['sigma'] - sigma_mean)
            vn = sub_sigma/(sub_sigma+delta_)
            dsh = dsh.append(vn*sigma_mean+(1-vn)*group['sigma'])
        return dsh[dtheta.index]


if __name__=="__main__":
    code_list = pd.Series([0.5,0.5],index=['000002.SZ','000005.SZ'])
    risk = BarraCNE5RiskModel.execute('20181028',code_list)
    print(risk)