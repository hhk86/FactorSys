'''
barra beta因子计算程序
'''
from operators.operator import Operator
from utils.data_util import *
import time

class PreBasicBalanceOperator(Operator):
    schema= 'basicdb'
    table = 'basic_balance'

    @classmethod
    def load_data(cls, date, code_list=None):

        ls = list(report_period_generator(period=30))
        print(ls)
        print("Program Stoped!")
        time.sleep(100000)
        # whole_df = pd.DataFrame(columns=["trade_date", "code"])
        # end_date = datetime.datetime.strftime(datetime.datetime.strptime(date, "%Y%m%d") + datetime.timedelta(10), "%Y%m%d")
        # # print(date, end_date)
        # # for factor in ['total_assets', 'total_equities_exc_min', 'total_equities_inc_min',
        # #             'noncur_liabilities', 'total_liabilities',
        # #             'longterm_loan', 'bonds_payable', 'longterm_payable', 'preferred_stock',
        # #             "cash", "tradable_financialasset", "notes_receiveable", "accounts_receivable",
        # #             "inventory", "fixed_asset", "construction_inprogress", "intangible_asset",
        # #             "development_expenditure", "goodwill", "notes_payable", "accounts_payable"]:
        # for factor in ['total_assets', 'total_equities_exc_min', 'total_equities_inc_min',
        #             'noncur_liabilities']:
        # # for factor in ["goodwill",]:
        #     df=  make_financial_factor(date, end_date, factor, test_mode=True)
        #     df.rename(columns={'tradeday': 'trade_date', 'ticker': 'code'}, inplace=True)
        #     df = df[df["trade_date"] == date]
        #     whole_df = pd.merge(whole_df, df, how="outer")
        #     pd.set_option("display.max_columns", None)
        # print(whole_df)
        # return whole_df

    @classmethod
    def fit(cls, datas):
        return datas

    @classmethod
    def dump_data(cls, datas):
        '''
        数据存储默认方法，如有不同存储方式，子类可重写该方法。
        :param datas: dataframe 待存储数据，必须有trade_date字段，且不为空
        :return:
        '''
        print(datas)
