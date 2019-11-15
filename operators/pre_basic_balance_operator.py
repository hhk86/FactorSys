'''
barra beta因子计算程序
'''
from common import db
from operators.operator import Operator
from utils.data_util import *
import time
import sys

class PreBasicBalanceOperator(Operator):
    schema= 'basicdb'
    table = 'basic_balance'

    @classmethod
    def load_data(cls, date, code_list=None):
        tuple_str = "("
        for s in report_period_generator(period=32):
            tuple_str += "'" + s + "',"
        tuple_str = tuple_str[:-1]
        tuple_str += ")"
        sql = '''
                SELECT
                    S_INFO_WINDCODE,
                    REPORT_PERIOD,
                    ACTUAL_ANN_DT,
                    STATEMENT_TYPE,
                    TOT_ASSETS AS total_assets,
                    TOT_SHRHLDR_EQY_EXCL_MIN_INT AS total_equities_exc_min,
                    TOT_SHRHLDR_EQY_INCL_MIN_INT AS total_equities_inc_min 
                FROM
                    wind.ASHAREBALANCESHEET
                WHERE
                    SUBSTR(S_INFO_WINDCODE, 1, 1) != 'A'
                    AND STATEMENT_TYPE in (408001000, 408004000, 408005000, 408050000)
                    AND REPORT_PERIOD in {}
        	'''.format(tuple_str)
        df = db.query_by_SQL("wind", sql)
        pd.set_option("display.max_columns", None)
        df = pd.merge(df, get_listed_stocks(), on="s_info_windcode")
        df.sort_values(by=["s_info_windcode", "report_period", "actual_ann_dt", "statement_type"], inplace=True)
        current_df = df.groupby(by="s_info_windcode").last()
        # current_df.reset_index(level=0, inplace=True)
        data_series = [(dt.datetime.strftime(dt.datetime.now(), "%Y%m%d"), list(current_df.index), current_df)]
        for snapshot_date in report_period_generator(period=20):
            print(snapshot_date)
            df_slice = df[(df["actual_ann_dt"] <= snapshot_date) & (df["report_period"] <= snapshot_date)].copy()
            df_slice.sort_values(by=["s_info_windcode", "report_period", "actual_ann_dt", "statement_type"], inplace=True)
            df_slice = df_slice.groupby(by="s_info_windcode").last()
            # df_slice.reset_index(level=0, inplace=True)
            data_series.append([snapshot_date, list(df_slice.index), df_slice])
        data_df = pd.DataFrame(data_series, columns=["date", "ticker_list", "report"])
        # df = pd.DataFrame(index=data_df.loc[0, "ticker_list"], columns=data_df.loc[0, "report"].columns[3:])
        df = data_df.loc[0, "report"]
        df.drop(["report_period", "actual_ann_dt", "statement_type"], axis=1, inplace=True)
        for factor in df.columns:
            df[factor + "_snapshots"] = [dict(), ] * df.shape[0]
        print(df)
        sys.exit()

        j = 0
        for ticker in data_df.loc[0, "ticker_list"]:
            level = 0
            while level < 20 and ticker in data_df.loc[level + 1, "ticker_list"]:
                level += 1
            while level > 0:
                # print(data_df.loc[level, "report"].columns)
                for factor in data_df.loc[level, "report"].columns[3:]:
                    df.loc[ticker, factor + "_snapshots"][data_df.loc[level, "date"]] = data_df.loc[level, "report"].loc[ticker, factor]
                    if level == 20 and factor == "total_assets":
                        print(data_df.loc[level, "report"].loc[ticker, factor])
                level -= 1
            j += 1
            print(j)
        print(df)

        df.to_csv("debug.csv")

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
