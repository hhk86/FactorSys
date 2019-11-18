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
        print("Downloading data ...")
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
                    TOT_SHRHLDR_EQY_INCL_MIN_INT AS total_equities_inc_min,
                    TOT_NON_CUR_LIAB AS noncur_liabilities,
                    TOT_LIAB AS total_liabilities,
                    LT_BORROW AS longterm_loan,
                    BONDS_PAYABLE AS bonds_payable,
                    LT_PAYABLE AS longterm_payable,
                    OTHER_EQUITY_TOOLS_P_SHR AS preferred_stock,
                    MONETARY_CAP AS cash,
                    TRADABLE_FIN_ASSETS AS tradable_financialasset,
                    NOTES_RCV AS notes_receiveable,
                    ACCT_RCV AS accounts_receivable,
                    INVENTORIES AS inventory,
                    FIX_ASSETS AS fixed_asset,
                    CONST_IN_PROG AS construction_inprogress,
                    INTANG_ASSETS AS intangible_asset,
                    R_AND_D_COSTS AS development_expenditure,
                    GOODWILL AS goodwill,
                    NOTES_PAYABLE AS notes_payable,
                    ACCT_PAYABLE AS accounts_payable
                FROM
                    wind.ASHAREBALANCESHEET
                WHERE
                    SUBSTR(S_INFO_WINDCODE, 1, 1) != 'A'
                    AND STATEMENT_TYPE in (408001000, 408004000, 408005000, 408050000)
                    AND REPORT_PERIOD in {}
        	'''.format(tuple_str)
        df = db.query_by_SQL("wind", sql)
        return df


    @classmethod
    def fit(cls, datas):
        df = datas
        print("Processing data ...")
        pd.set_option("display.max_columns", None)
        df = pd.merge(df, get_listed_stocks(), on="s_info_windcode")
        df.sort_values(by=["s_info_windcode", "report_period", "actual_ann_dt", "statement_type"], inplace=True)
        current_df = df.groupby(by="s_info_windcode").last()
        data_series = [(dt.datetime.strftime(dt.datetime.now(), "%Y%m%d"), list(current_df.index), current_df)]
        for snapshot_date in report_period_generator(period=20):
            df_slice = df[(df["actual_ann_dt"] <= snapshot_date) & (df["report_period"] <= snapshot_date)].copy()
            df_slice.sort_values(by=["s_info_windcode", "report_period", "actual_ann_dt", "statement_type"],
                                 inplace=True)
            df_slice = df_slice.groupby(by="s_info_windcode").last()
            data_series.append([snapshot_date, list(df_slice.index), df_slice])
        data_df = pd.DataFrame(data_series, columns=["date", "ticker_list", "report"])
        for ticker in data_df.loc[0, "ticker_list"]:
            level = 0
            while level < 20 and ticker in data_df.loc[level + 1, "ticker_list"]:
                level += 1
            data_df.loc[0, "report"].loc[ticker, "max_level"] = level
        data_df.loc[0, "report"]["max_level"] = data_df.loc[0, "report"]["max_level"].astype(int)
        df = data_df.loc[0, "report"].copy()
        df.drop(["report_period", "actual_ann_dt", "statement_type", "max_level"], axis=1, inplace=True)
        for factor in df.columns:
            df[factor + "_snapshots"] = [dict(), ] * df.shape[0]
        for col in df.columns:
            if col.endswith("_snapshots"):
                for index in df.index:
                    df.set_value(index, col, dict())

        print("Making snapshots ...")
        for ticker in data_df.loc[0, "ticker_list"]:
            max_level = data_df.loc[0, "report"].loc[ticker, "max_level"]
            for factor in data_df.loc[0, "report"].columns[
                          3: -1]:  # Exclude ["report_period", "actual_ann_dt", "statement_type", "max_level"]
                for level in range(max_level, 0, -1):
                    df.loc[ticker, factor + "_snapshots"][data_df.loc[level, "date"]] = \
                    data_df.loc[level, "report"].loc[ticker, factor]
        for col in df.columns:
            if col.endswith("_snapshots"):
                df[col] = df[col].astype(str)
        print("Converting some NaN to 0 ...")
        for factor in ['total_assets', 'total_equities_exc_min', 'total_equities_inc_min',
                       'noncur_liabilities', 'total_liabilities',
                       'longterm_loan', 'bonds_payable', 'longterm_payable', 'preferred_stock',
                       "cash", "tradable_financialasset", "notes_receiveable", "accounts_receivable",
                       "inventory", "fixed_asset", "construction_inprogress", "intangible_asset",
                       "development_expenditure", "goodwill", "notes_payable", "accounts_payable"]:
            df[factor + "_snapshots"] = df[factor + "_snapshots"].apply(lambda s: s.replace("{}", ''))
            if factor not in ['total_assets', 'total_equities_exc_min', 'total_equities_inc_min',
                              'noncur_liabilities', 'total_liabilities']:
                df[factor] = df[factor].apply(lambda x: 0 if pd.isna(x) else x)
                df[factor + "_snapshots"] = df[factor + "_snapshots"].apply(lambda s: s.replace("nan", '0'))

        df.to_csv("debug.csv")


        return df

    @classmethod
    def dump_data(cls, datas):
        '''
        数据存储默认方法，如有不同存储方式，子类可重写该方法。
        :param datas: dataframe 待存储数据，必须有trade_date字段，且不为空
        :return:
        '''
        print(datas)
