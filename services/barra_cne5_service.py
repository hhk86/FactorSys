import numpy as np
from models.barra_cne5_ret_model import BarraCNE5RetModel as __BarraCNE5RetModel


class BarraCNE5RetService(__BarraCNE5RetModel):

    @classmethod
    def load_data(cls, date, code_list=None):
        cls.cache(date,code_list)
        return date
    @classmethod
    def fit(cls, datas):
        x = cls.factors.join(cls.weight, how='inner')
        W = np.diag(x['weight'])
        x.drop(['weight'], axis=1, inplace=True)
        X = np.dot(x, cls.R)
        portfolio = cls.R.dot(np.linalg.inv(X.T.dot(W).dot(X)).dot(X.T).dot(W))
        portfolio.columns = x.index
        portfolio.drop(index=cls.industries,inplace=True)
        portfolio.drop(index='const',inplace=True)
        return portfolio.T,datas
    @classmethod
    def dump_data(cls, datas):
        #根据格式要求转换数据，并将数据传至前端调用的handler
        datas[0].to_csv('barra-%s.csv' % datas[1])