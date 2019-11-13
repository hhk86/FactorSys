'''
barra beta因子计算程序
'''
from operators.operator import Operator
from utils.data_util import *


class PreBasicIncomeOperator(Operator):
    schema= 'basicdb'
    table = 'basic_income'

    @classmethod
    def load_data(cls, date, code_list=None):
        whole_df = pd.DataFrame(columns=["trade_date", "code"])
        end_date = datetime.datetime.strftime(datetime.datetime.strptime(date, "%Y%m%d") + datetime.timedelta(10), "%Y%m%d")
        # print(date, end_date)
        for factor in ["net_income", "total_revenue",
                "revenue", "total_opcost", "operating_cost", "sale_expense",
                "management_expense", "research_expense", "financial_expense", "operating_profit"]:
            df=  make_financial_factor(date, end_date, factor, test_mode=False)
            df.rename(columns={'tradeday': 'trade_date', 'ticker': 'code'}, inplace=True)
            df = df[df["trade_date"] == date]
            whole_df = pd.merge(whole_df, df, how="outer")
        return whole_df

    @classmethod
    def fit(cls, datas):
        return datas

    # @classmethod
    # def dump_data(cls, datas):
    #     '''
    #     数据存储默认方法，如有不同存储方式，子类可重写该方法。
    #     :param datas: dataframe 待存储数据，必须有trade_date字段，且不为空
    #     :return:
    #     '''
    #     print(datas)
