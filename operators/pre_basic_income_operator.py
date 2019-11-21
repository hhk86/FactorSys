import pandas as pd
import datetime as dt
from common import db
from operators.operator import Operator
from utils.data_util import report_period_generator, get_listed_stocks, Quarters2LastDec
import warnings
warnings.simplefilter(action='ignore', category=FutureWarning)


class PreBasicIncomeOperator(Operator):
    schema= 'basicdb'
    table = 'basic_income'

    @classmethod
    def load_data(cls, date, code_list=None):
        print("Downloading data ...")
        tuple_str = "("
        for s in report_period_generator(period=32, date=date):
            tuple_str += "'" + s + "',"
        tuple_str = tuple_str[:-1]
        tuple_str += ")"
        sql = '''
                SELECT
                    S_INFO_WINDCODE,
                    REPORT_PERIOD,
                    ACTUAL_ANN_DT,
                    STATEMENT_TYPE,
                    NET_PROFIT_EXCL_MIN_INT_INC AS net_income,
                    TOT_OPER_REV AS total_revenue,
                    OPER_REV AS revenue,
                    TOT_OPER_COST AS total_opcost,
                    LESS_OPER_COST AS operating_cost,
                    LESS_SELLING_DIST_EXP AS sale_expense,
                    LESS_GERL_ADMIN_EXP AS management_expense,
                    RD_EXPENSE AS research_expense,
                    LESS_FIN_EXP AS financial_expense,
                    OPER_PROFIT AS operating_profit
                FROM
                    wind.ASHAREINCOME
                WHERE
                    SUBSTR(S_INFO_WINDCODE, 1, 1) != 'A'
                    AND STATEMENT_TYPE in (408001000, 408004000, 408005000, 408050000)
                    AND REPORT_PERIOD in {0}
                    AND ACTUAL_ANN_DT <= {1}
        	'''.format(tuple_str, date)
        df = db.query_by_SQL("wind", sql)
        return df, date


    @classmethod
    def fit(cls, datas):
        print("Processing data ...")
        df, date = datas
        df = pd.merge(df, get_listed_stocks(), on="s_info_windcode")
        df.sort_values(by=["s_info_windcode", "report_period", "actual_ann_dt", "statement_type"], inplace=True)
        current_df = df.groupby(by="s_info_windcode").last()
        data_series = [(dt.datetime.strftime(dt.datetime.now(), "%Y%m%d"), list(current_df.index), current_df)]
        for snapshot_date in report_period_generator(period=24, date=date):
            # df_slice = df[(df["actual_ann_dt"] <= snapshot_date) & (df["report_period"] <= snapshot_date)].copy()
            df_slice = df[df["report_period"] <= snapshot_date].copy()
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

        print("Making snapshots ...")
        ttm_df = pd.DataFrame(columns=list(data_df.loc[0, "report"].columns)[3: -1])
        for ticker in data_df.loc[0, "ticker_list"]:
            if data_df.loc[0, "report"].loc[ticker, "max_level"] >= 5:
                shift = Quarters2LastDec(date)
                ttm_df.loc[ticker, :] = data_df.loc[0, "report"].ix[ticker, 3:] \
                                        + data_df.loc[shift, "report"].ix[ticker, 3:] - data_df.loc[5, "report"].ix[ticker, 3:]
        ttm_df = ttm_df.reset_index()
        ttm_df.rename(columns={'index': 'code'}, inplace=True)
        ttm_df["trade_date"] = date
        ttm_df["trade_date"] = date
        new_cols = list(ttm_df.columns)
        new_cols.insert(0, new_cols.pop(new_cols.index("trade_date")))
        ttm_df = ttm_df[new_cols]

        for col in ttm_df.columns[2:]:
            ttm_df[col +"_snapshots"] = None
        for col in ttm_df.columns:
            if col.endswith("_snapshots"):
                for index in ttm_df.index:
                    ttm_df.set_value(index, col, dict())
        ttm_df.index = ttm_df["code"]
        for k in range(20, 0, -1):
            snapshot_date = data_df.loc[k, "date"]
            shift = Quarters2LastDec(snapshot_date)
            for ticker in data_df.loc[0, "ticker_list"]:
                if data_df.loc[0, "report"].loc[ticker, "max_level"] >= k + 4:
                    for col in data_df.loc[k + shift, "report"].columns[3: ]:
                        ttm_df.loc[ticker, col + "_snapshots"][snapshot_date] = data_df.loc[k, "report"].loc[ticker, col] \
                                + data_df.loc[k + shift, "report"].loc[ticker, col] - data_df.loc[k + 4, "report"].loc[ticker, col]

        print("Converting some NaN to 0 ...")
        df = ttm_df
        for col in df.columns:
            if col.endswith("_snapshots"):
                df[col] = df[col].astype(str)
        for factor in ["net_income", "total_revenue", "revenue", "total_opcost", "operating_cost", "sale_expense",
                "management_expense", "research_expense", "financial_expense", "operating_profit"]:
            df[factor + "_snapshots"] = df[factor + "_snapshots"].apply(lambda s: s.replace("{}", ''))
            if factor not in ["net_income", "total_revenue"]:
                df[factor] = df[factor].apply(lambda x: 0 if pd.isna(x) else x)
                df[factor + "_snapshots"] = df[factor + "_snapshots"].apply(lambda s: s.replace("nan", '0'))

        return df

    # @classmethod
    # def dump_data(cls, datas):
    #     '''
    #     数据存储默认方法，如有不同存储方式，子类可重写该方法。
    #     :param datas: dataframe 待存储数据，必须有trade_date字段，且不为空
    #     :return:
    #     '''
    #     print(datas)
