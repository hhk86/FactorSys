from common import db as __db
from datetime import datetime as __datetime
from dateutil.relativedelta import relativedelta as __relativedelta
from calendar import monthrange as __monthrange
from datetime import datetime as dt
import datetime as dt
import datetime
import pandas as pd
import numpy as np
from dateutil.relativedelta import relativedelta as td


def get_suspend_list(date):
    sus = __db.query_MongoDB('factor', 'universedb', 'universe', {'trade_date':date, 'suspend':True}, {'code':1, '_id':0})
    return [] if sus.shape[1]==0 else list(sus['code'])

def nearby_season_month(datestr, offset):
    date = __datetime.strptime(datestr, '%Y%m%d')
    date = date.replace(day=1)
    season_month = date.replace(month=3 * (date.month // 3 + int(date.month % 3 > 0))) + __relativedelta(months=3 * offset)
    return season_month.replace(day=__monthrange(season_month.year, season_month.month)[1]).strftime('%Y%m%d')


# The following part contains the classes and functions extracted from "Barra" folder, which is used for querying data from financial statements.
class FactorNode(object):
    def __init__(self, ann_date, factor):
        self.ann_date = ann_date
        if pd.isna(factor):
            factor = None # float('nan')
        self.factor = factor

    def show(self):
        print(self.ann_date, self.factor)


class FactorStack(object):
    def __init__(self, ana_date=None, factor=None):
        self.values = []

        if ana_date:
            if not pd.isna(factor):
                self.push(ana_date, factor)
            else:
                self.push(ana_date, None)
        else:
            raise Exception('ann_date missing')

    def push(self, ana_date, factor):
        self.values.append(FactorNode(ana_date, factor))

    def top(self):
        try:
            return self.values[-1]
        except Exception as e:
            raise Exception('FactorStack.top')

    def show(self):
        for factor_node in self.values:
            factor_node.show()


class StkNode(object):
    def __init__(self, ticker, report_period, ana_date, factor):
        self.ticker = ticker
        self.latest_report = report_period
        self.values = {report_period: FactorStack(ana_date,factor)}

    def push(self, report_period, ana_date, factor):
        if report_period > self.latest_report:
            self.values[report_period] = FactorStack(ana_date, factor)
            self.latest_report = report_period
        else:
            try:
                self.values[report_period].push(ana_date, factor)
            except KeyError:
                self.values[report_period] = FactorStack(ana_date, factor)
                self.values[report_period].push(ana_date, factor)
                print(self.ticker + '的' + report_period + '报告晚于通常情况')
            except Exception as e:
                print(e)
                raise Exception('StkNode.push')

    def get(self, report_period):
        return self.values[report_period].top()

    def get_latest_report(self):
        return self.latest_report

    def top(self):
        return self.values[self.latest_report].top()

    def show(self):
        for report_period, factorstack in self.values.items():
            print('Report Period: '+ report_period)
            factorstack.show()


class FactorsPort(object):
    def __init__(self, tickers):
        self._data = dict.fromkeys(tickers, None)

    def push(self, ticker, report_period, ana_date, factor):
        if not self._data[ticker]:
            self._data[ticker] = StkNode(ticker, report_period, ana_date, factor)
        else:
            self._data[ticker].push(report_period, ana_date, factor)
        pass

    def get(self, ticker, report_period):
        return self._data[ticker].get(report_period)

    def get_latest_report(self, ticker):
        try:
            return self._data[ticker].get_latest_report()
        except AttributeError:
            # self._data全部为None值
            return
        except Exception:
            raise Exception('FactorsPort.get_latest_report')

    def top(self, ticker):
        return self._data[ticker].top()

    def show(self):
        for k, v in self._data.items():
            print("Code is " + k)
            if v is not None:
                v.show()

    def snap(self, ticker, report_date):
        all_report_dates = self._data[ticker].values.keys()
        report_date = datetime.datetime.strptime(report_date, '%Y%m%d')
        start_report_date = report_date.replace(year=report_date.year - 6).strftime('%Y%m%d')

        results = {}
        for rdate in all_report_dates:
            if rdate >= start_report_date:
                try:
                    results[rdate] = self._data[ticker].values[rdate].top().factor
                except:
                    print('haha')

        if results:
            return str(results)
        else:
            return ''


def calendar(date0, date1):
    sql = '''SELECT trade_days FROM wind.ASHARECALENDAR 
        WHERE TRADE_DAYS>='%s' AND TRADE_DAYS<='%s' AND S_INFO_EXCHMARKET='SSE'
        ORDER BY TRADE_DAYS''' % (date0, date1)
    data = __db.query_by_SQL("wind", sql).squeeze().tolist()
    return data


def universe(date):
    sql = '''SELECT S_INFO_WINDCODE FROM wind.ASHAREDESCRIPTION WHERE S_INFO_LISTDATE IS NOT NULL 
             AND S_INFO_LISTDATE <'%s' AND S_INFO_DELISTDATE IS NULL ORDER BY S_INFO_WINDCODE''' % (date)
    data = __db.query_by_SQL("wind", sql).squeeze().tolist()
    return data


def _report_range_min(min_date):
    start_date = dt.strptime(min_date, '%Y%m%d')
    return dt(start_date.year - 7, 9, 30).strftime('%Y%m%d')

def _date_range_limits(date_range):
    if isinstance(date_range, (bytes, str)):
        date_range = [date_range]
    date = list(map(lambda s: dt.strptime(s, '%Y%m%d'), date_range))
    return min(date).strftime('%Y%m%d'), max(date).strftime('%Y%m%d')

def _coderange2str(code_range):
    if isinstance(code_range, (bytes, str)):
        code_range = [code_range]
    return '\'' + '\',\''.join(code_range) + '\''


def _generate_season_table(min_date, max_date):
    d0 = _nearby_season_month(min_date, -1)
    d1 = _nearby_season_month(max_date, 0)
    datelist = [d0]
    while datelist[-1] < d1:
        datelist.append(_nearby_season_month(datelist[-1], 1))
    return datelist

def _nearby_season_month(datestr, offset):
    date = dt.strptime(datestr, '%Y%m%d')
    date = date.replace(day=1)
    season_month = date.replace(month=3 * (date.month // 3 + int(date.month % 3 > 0))) + td(months=3 * offset)
    return season_month.replace(day=__monthrange(season_month.year, season_month.month)[1]).strftime('%Y%m%d')

def _prev_quarter(datestr):
    return _nearby_season_month(datestr, -1)

def _prev_annual(datestr):
    date = dt.strptime(datestr, '%Y%m%d')
    return date.replace(year=date.year - 1, month=12, day=31).strftime('%Y%m%d')

def _lastyear_quarter(datestr):
    return _nearby_season_month(datestr, -4)

def _find_eff_date(date_range):
    ipos = 0
    while ipos < len(date_range):
        basedate = yield ipos, date_range[ipos]
        while date_range[ipos] <= basedate:
            ipos = ipos + 1


def balanceRaw(code_range, date_range, factor, factor_method='avg'):
    def _factor2str(factor):
        if factor == 'total_assets':  # 总资产
            return 'TOT_ASSETS'
        elif factor == "total_equities_exc_min":  # 股东权益-不含少数股东权益
            return "TOT_SHRHLDR_EQY_EXCL_MIN_INT"
        elif factor == 'total_equities_inc_min':  # 股东权益-含少数股东权益
            return 'TOT_SHRHLDR_EQY_INCL_MIN_INT'
        elif factor == "noncur_liabilities":  # 非流动性负债
            return "TOT_NON_CUR_LIAB"
        elif factor == "total_liabilities":  # 总负债-即负债科目
            return "TOT_LIAB"
        elif factor == 'longterm_loan':  # 长期负债
            return 'LT_BORROW'
        elif factor == 'bonds_payable':  # 应付贷款
            return 'BONDS_PAYABLE'
        elif factor == 'longterm_payable':  # 长期应付款
            return 'LT_PAYABLE'
        elif factor == 'preferred_stock':  # 优先股
            return 'OTHER_EQUITY_TOOLS_P_SHR'
        elif factor == "cash":  # 货币资金
            return "MONETARY_CAP"
        elif factor == "tradable_financialasset":  # 交易性金融资产
            return "TRADABLE_FIN_ASSETS"
        elif factor == "notes_receiveable":  # 应收票据
            return "NOTES_RCV"
        elif factor == "accounts_receivable":  # 应收账款
            return "ACCT_RCV"
        elif factor == "inventory":  # 存货
            return "INVENTORIES"
        elif factor == "fixed_asset":  # 固定资产
            return "FIX_ASSETS"
        elif factor == "construction_inprogress":  # 在建工程
            return "CONST_IN_PROG"
        elif factor == "intangible_asset":  # 无形资产
            return "INTANG_ASSETS"
        elif factor == "development_expenditure":  # 开发支出
            return "R_AND_D_COSTS"
        elif factor == "goodwill":  # 商誉
            return "GOODWILL"
        elif factor == "notes_payable":  # 应付票据
            return "NOTES_PAYABLE"
        elif factor == "accounts_payable":  # 应付账款
            return "ACCT_PAYABLE"

    code_str = _coderange2str(code_range)
    min_date, max_date = _date_range_limits(date_range)
    factor_str = _factor2str(factor)

    report_period_start = _report_range_min(min_date)

    sql = "SELECT S_INFO_WINDCODE,ANN_DT,ACTUAL_ANN_DT,REPORT_PERIOD,STATEMENT_TYPE," + factor_str \
          + """ FROM wind.ASHAREBALANCESHEET
              WHERE STATEMENT_TYPE IN ('408001000','408004000','408005000') 
                    AND S_INFO_WINDCODE IN (%s) AND REPORT_PERIOD >= '%s'
                    AND ACTUAL_ANN_DT < '%s'
              ORDER BY S_INFO_WINDCODE,REPORT_PERIOD,ACTUAL_ANN_DT""" % (
              code_str, report_period_start, max_date)
    columns = ['Code', 'AnnDate', 'ActualAnnDate', 'ReportPeriod', 'StatementType', factor_str]


    data = __db.query_by_SQL("wind", sql)

    data.columns = columns
    data = data.set_index(['Code', 'ReportPeriod', 'ActualAnnDate'])
    data = data.sort_index(level=['ActualAnnDate', 'Code', 'ReportPeriod'])

    factor_port = FactorsPort(code_range)

    for item in data[data.index.get_level_values('ActualAnnDate') < date_range[0]].iterrows():
        # item[0]: code, reportperiod, actualanndate
        factor_port.push(item[0][0], item[0][1], item[0][2], item[1][factor_str])

    # latest_released_report为在指定交易日已知的最新财务报表报告期
    latest_released_report = pd.DataFrame([], columns=code_range, index=date_range)
    # latest_factor_value为最新财务报表的数据
    factor_values = pd.DataFrame([], columns=code_range, index=date_range)
    factor_values[factor_values.isnull()] = None

    snapshots = pd.DataFrame([], columns=code_range, index=date_range)

    season_table = pd.DataFrame([], columns=['prevQuarter', 'prevAnnual', 'lastyearQuarter'], \
                                index=_generate_season_table(report_period_start, max_date))

    season_table['prevQuarter'] = season_table.index.map(lambda s: _prev_quarter(s))
    season_table['prevAnnual'] = season_table.index.map(lambda s: _prev_annual(s))
    season_table['lastyearQuarter'] = season_table.index.map(lambda s: _lastyear_quarter(s))

    date_beginning = date_range[0]

    data = data.reset_index().set_index(['ActualAnnDate', 'Code'])
    data = data.sort_index(level=['ActualAnnDate', 'Code'])

    # 产生初始值的公告日
    ann_date_container = data.index.get_level_values('ActualAnnDate').drop_duplicates().values
    ann_date_container = ann_date_container[ann_date_container >= date_beginning]

    func = _find_eff_date(date_range)
    func.send(None)

    # # 以下计算初始值
    for code in latest_released_report.columns:
        if not factor_port.get_latest_report(code):
            continue

        latest_released_report.loc[latest_released_report.index[0], code] = factor_port.get_latest_report(code)
        if factor_method in ('avg'):
            try:
                lastyearQuater = season_table.loc[
                    latest_released_report.loc[latest_released_report.index[0], code], 'lastyearQuarter']
                factor_values.loc[factor_values.index[0], code] = 0.5 * factor_port.top(code).factor \
                                                                  + 0.5 * factor_port.get(code, lastyearQuater).factor
            except KeyError:
                factor_values.loc[factor_values.index[0], code] = None
        elif factor_method == 'latest':
            factor_values.loc[factor_values.index[0], code] = factor_port.top(code).factor

        snapshots.loc[factor_values.index[0], code] = factor_port.snap(code, factor_port.get_latest_report(code))

    # print('Initialization Finished')

    for ann_date in ann_date_container:
        ipos, eff_date = func.send(ann_date)
        new_info = data.loc[ann_date, ['ReportPeriod', factor_str]]
        for new_factor in new_info.iterrows():
            # 需要更新处理的新信息：code, report_period, ann_date, factor
            code, report_period, factor = new_factor[0], new_factor[1]['ReportPeriod'], new_factor[1][factor_str]
            # 若数据过于陈旧，则本次更新取消，这里是非常陈旧，例如update 90年代的年报
            if report_period < report_period_start:
                continue

            # 新数据推送入库之前的最新报告期
            old_latest_report = factor_port.get_latest_report(code)
            # 新数据推送入库
            factor_port.push(code, report_period, ann_date, factor)
            # 新数据推送入库后的最新报告期
            new_latest_report = factor_port.get_latest_report(code)

            snapshots.loc[eff_date, code] = factor_port.snap(code, factor_port.get_latest_report(code))
            # 至此，存在3个报告期，old_latest_report, report_period, new_latest_report
            # 如果：
            # 情形一：report_period < old_latest_report, 则 report_period < old_latest_report = new_latest_report
            # 情形二：report_period = old_latest_report，则 report_period = old_latest_report = new_latest_report
            # 情形三：report_period > old_latest_report，则 old_latest_report < report_period = new_latest_report
            # 更新factor_values值
            if (old_latest_report) and (report_period <= old_latest_report):
                # 本次推送的数据是对之前报告数据的更新
                assert old_latest_report == new_latest_report, "Error in old_latest_report == new_latest_report"
                if factor_method in ('avg'):
                    try:
                        lastyearQuater_value = factor_port.get(code, season_table.loc[
                            old_latest_report, 'lastyearQuarter']).factor
                        factor_values.loc[eff_date, code] = 0.5 * factor_port.get(code, old_latest_report).factor \
                                                            + 0.5 * lastyearQuater_value
                    except KeyError:
                        factor_values.loc[eff_date, code] = None
                elif factor_method == 'latest':
                    factor_values.loc[eff_date, code] = factor_port.get(code, old_latest_report).factor
            else:
                assert report_period == new_latest_report, "Error in report_period == new_latest_report"
                # 新信息为新发布的财务报告
                latest_released_report.loc[eff_date, code] = new_latest_report
                if factor_method in ('avg'):
                    try:
                        lastyearQuater_value = factor_port.get(code, season_table.loc[
                            new_latest_report, 'lastyearQuarter']).factor
                        factor_values.loc[eff_date, code] = 0.5 * factor_port.get(code, new_latest_report).factor \
                                                            + 0.5 * lastyearQuater_value
                    except KeyError:
                        factor_values.loc[eff_date, code] = None
                elif factor_method == 'latest':
                    factor_values.loc[eff_date, code] = factor_port.get(code, new_latest_report).factor

    latest_released_report = latest_released_report.fillna(method='ffill')
    factor_values = factor_values.fillna(method='ffill')
    factor_values[factor_values.isnull()] = None
    snapshots = snapshots.fillna(method='ffill')

    return factor_values, snapshots, latest_released_report, data


def incomeRaw(code_range, date_range, factor, factor_method='ttm'):
    def _factor2str(factor):
        if factor == 'net_income': # 净利润
            return 'NET_PROFIT_EXCL_MIN_INT_INC'
        elif factor == 'total_revenue': # 营业总收入
            return 'TOT_OPER_REV'
        elif factor == "revenue": # 营业收入
            return "OPER_REV"
        elif factor == "total_opcost": # 营业总成本
            return "TOT_OPER_COST"
        elif factor == "operating_cost": # 营业成本呢
            return "LESS_OPER_COST"
        elif factor == "sale_expense": # 销售费用
            return "LESS_SELLING_DIST_EXP"
        elif factor == "management_expense": # 管理费用
            return "LESS_GERL_ADMIN_EXP"
        elif factor == "research_expense": # 研发费用
            return "RD_EXPENSE"
        elif factor == "financial_expense": # 财务费用
            return "LESS_FIN_EXP"
        elif factor == "operating_profit": # 营业利润
            return "OPER_PROFIT"

    code_str = _coderange2str(code_range)
    min_date, max_date = _date_range_limits(date_range)
    factor_str = _factor2str(factor)

    report_period_start = _report_range_min(min_date)

    sql = "SELECT S_INFO_WINDCODE,ANN_DT,ACTUAL_ANN_DT,REPORT_PERIOD,STATEMENT_TYPE," + factor_str \
          + """ FROM wind.ASHAREINCOME
              WHERE STATEMENT_TYPE IN ('408001000','408004000','408005000') 
                    AND S_INFO_WINDCODE IN (%s) AND REPORT_PERIOD >= '%s'
                    AND ACTUAL_ANN_DT < '%s'
              ORDER BY S_INFO_WINDCODE,REPORT_PERIOD,ACTUAL_ANN_DT""" % (
          code_str, report_period_start, max_date)
    columns = ['Code', 'AnnDate', 'ActualAnnDate', 'ReportPeriod', 'StatementType', factor_str]


    data = __db.query_by_SQL("wind", sql)

    data.columns = columns
    data = data.set_index(['Code', 'ReportPeriod', 'ActualAnnDate'])
    data = data.sort_index(level=['ActualAnnDate', 'Code', 'ReportPeriod'])

    factor_port = FactorsPort(code_range)

    for item in data[data.index.get_level_values('ActualAnnDate') < date_range[0]].iterrows():
        # item[0]: code, reportperiod, actualanndate
        factor_port.push(item[0][0], item[0][1], item[0][2], item[1][factor_str])

    # latest_released_report为在指定交易日已知的最新财务报表报告期
    latest_released_report = pd.DataFrame([], columns=code_range, index=date_range)
    # latest_factor_value为最新财务报表的数据
    factor_values = pd.DataFrame([], columns=code_range, index=date_range)
    snapshots = pd.DataFrame([], columns=code_range, index=date_range)

    season_table = pd.DataFrame([], columns=['prevQuarter', 'prevAnnual', 'lastyearQuarter'], \
                                index=_generate_season_table(report_period_start, max_date))

    season_table['prevQuarter'] = season_table.index.map(lambda s: _prev_quarter(s))
    season_table['prevAnnual'] = season_table.index.map(lambda s: _prev_annual(s))
    season_table['lastyearQuarter'] = season_table.index.map(lambda s: _lastyear_quarter(s))

    date_beginning = date_range[0]

    data = data.reset_index().set_index(['ActualAnnDate', 'Code'])
    data = data.sort_index(level=['ActualAnnDate', 'Code'])

    # 产生初始值的公告日
    ann_date_container = data.index.get_level_values('ActualAnnDate').drop_duplicates().values
    ann_date_container = ann_date_container[ann_date_container >= date_beginning]

    func = _find_eff_date(date_range)
    func.send(None)

    # # 以下计算初始值
    for code in latest_released_report.columns:
        if not factor_port.get_latest_report(code):
            continue
        latest_released_report.loc[latest_released_report.index[0], code] = factor_port.get_latest_report(code)

        if factor_method in ('TTM', 'ttm'):
            if latest_released_report.loc[latest_released_report.index[0], code][4:6] == '12':
                factor_values.loc[factor_values.index[0], code] = factor_port.top(code).factor
            else:
                try:
                    factor_values.loc[factor_values.index[0], code] = factor_port.top(code).factor \
                        + factor_port.get(code, season_table.loc[latest_released_report.loc[latest_released_report.index[0], code], 'prevAnnual']).factor \
                        - factor_port.get(code, season_table.loc[latest_released_report.loc[latest_released_report.index[0], code], 'lastyearQuarter']).factor
                except KeyError:
                    factor_values.loc[factor_values.index[0], code] = None
                except TypeError:
                    if factor in ['net_income', 'total_revenue', 'revenue', 'operating_profit']:
                        factor_values.loc[factor_values.index[0], code] = None
                    else:
                        try:
                            latest = factor_port.top(code).factor
                            prevAnnual = factor_port.get(code, season_table.loc[latest_released_report.loc[latest_released_report.index[0], code], 'prevAnnual']).factor
                            lastyearQuarter = factor_port.get(code, season_table.loc[latest_released_report.loc[latest_released_report.index[0], code], 'lastyearQuarter']).factor

                            latest = .0 if latest is None else latest
                            prevAnnual = .0 if prevAnnual is None else prevAnnual
                            lastyearQuarter = .0 if lastyearQuarter is None else lastyearQuarter

                            factor_values.loc[factor_values.index[0], code] = latest + prevAnnual - lastyearQuarter
                        except KeyError:
                            factor_values.loc[factor_values.index[0], code] = None
        snapshots.loc[factor_values.index[0], code] = factor_port.snap(code, factor_port.get_latest_report(code))

    # print('Initialization Finished')

    for ann_date in ann_date_container:
        ipos, eff_date = func.send(ann_date)
        new_info = data.loc[ann_date, ['ReportPeriod', factor_str]]
        for new_factor in new_info.iterrows():
            # 需要更新处理的新信息：code, report_period, ann_date, factor
            code, report_period, factor = new_factor[0], new_factor[1]['ReportPeriod'], new_factor[1][factor_str]
            # 若数据过于陈旧，则本次更新取消
            if report_period < report_period_start:
                continue

            # 新数据推送入库之前的最新报告期
            old_latest_report = factor_port.get_latest_report(code)
            # 新数据推送入库
            factor_port.push(code, report_period, ann_date, factor)
            # 新数据推送入库后的最新报告期
            new_latest_report = factor_port.get_latest_report(code)

            # print(ann_date)

            snapshots.loc[eff_date, code] = factor_port.snap(code, factor_port.get_latest_report(code))
            # 至此，存在3个报告期，old_latest_report, report_period, new_latest_report
            # 如果：
            # 情形一：report_period < old_latest_report, 则 report_period < old_latest_report = new_latest_report
            # 情形二：report_period = old_latest_report，则 report_period = old_latest_report = new_latest_report
            # 情形三：report_period > old_latest_report，则 old_latest_report < report_period = new_latest_report
            # 更新factor_values值
            if (old_latest_report) and (report_period <= old_latest_report):
                # 本次推送的数据是对之前报告数据的更新
                assert old_latest_report == new_latest_report, "Error in old_latest_report == new_latest_report"
                if factor_method in ('TTM', 'ttm'):
                    if old_latest_report[4:6] == '12':
                        factor_values.loc[eff_date, code] = factor_port.get(code, old_latest_report).factor
                    else:
                        try:
                            factor_values.loc[eff_date, code] = factor_port.get(code, old_latest_report).factor \
                                + factor_port.get(code, season_table.loc[old_latest_report, 'prevAnnual']).factor \
                                - factor_port.get(code, season_table.loc[old_latest_report, 'lastyearQuarter']).factor
                        except KeyError:
                            factor_values.loc[eff_date, code] = None
                        except TypeError:
                            if factor in ['net_income', 'total_revenue', 'revenue', 'operating_profit']:
                                factor_values.loc[eff_date, code] = None
                            else:
                                try:
                                    latest = factor_port.get(code, new_latest_report).factor
                                    prevAnnual = factor_port.get(code, season_table.loc[new_latest_report, 'prevAnnual']).factor
                                    lastyearQuarter = factor_port.get(code, season_table.loc[new_latest_report, 'lastyearQuarter']).factor

                                    latest = .0 if latest is None else latest
                                    prevAnnual = .0 if prevAnnual is None else prevAnnual
                                    lastyearQuarter = .0 if lastyearQuarter is None else lastyearQuarter

                                    factor_values.loc[eff_date, code] = latest + prevAnnual - lastyearQuarter
                                except KeyError:
                                    factor_values.loc[eff_date, code] = None
            else:
                assert report_period == new_latest_report, "Error in report_period == new_latest_report"
                # 新信息为新发布的财务报告
                latest_released_report.loc[eff_date, code] = new_latest_report
                if factor_method in ('TTM', 'ttm'):
                    if new_latest_report[4:6] == '12':
                        factor_values.loc[eff_date, code] = factor_port.get(code, new_latest_report).factor
                    else:
                        try:
                            factor_values.loc[eff_date, code] = factor_port.get(code, new_latest_report).factor \
                                + factor_port.get(code, season_table.loc[new_latest_report, 'prevAnnual']).factor \
                                - factor_port.get(code, season_table.loc[new_latest_report, 'lastyearQuarter']).factor
                        except KeyError:
                            factor_values.loc[eff_date, code] = None
                        except TypeError:
                            if factor in ['net_income', 'total_revenue', 'revenue', 'operating_profit']:
                                factor_values.loc[eff_date, code] = None
                            else:
                                try:
                                    latest =  factor_port.get(code, new_latest_report).factor
                                    prevAnnual = factor_port.get(code, season_table.loc[new_latest_report, 'prevAnnual']).factor
                                    lastyearQuarter = factor_port.get(code, season_table.loc[new_latest_report, 'lastyearQuarter']).factor

                                    latest = .0 if latest is None else latest
                                    prevAnnual = .0 if prevAnnual is None else prevAnnual
                                    lastyearQuarter = .0 if lastyearQuarter is None else lastyearQuarter

                                    factor_values.loc[eff_date, code] = latest + prevAnnual - lastyearQuarter
                                except KeyError:
                                    factor_values.loc[eff_date, code] = None

    latest_released_report = latest_released_report.fillna(method='ffill')
    factor_values = factor_values.fillna(method='ffill')
    snapshots = snapshots.fillna(method='ffill')

    return factor_values, snapshots, latest_released_report, data


def cashflowRaw(code_range, date_range, factor, factor_method='ttm'):
    def _factor2str(factor):
        if factor == 'operating_cashflow':  # 经营活动现金流
            return 'NET_CASH_FLOWS_OPER_ACT'
        elif factor == 'operating_cashinflow':  # 经营活动现金流入
            return 'STOT_CASH_INFLOWS_OPER_ACT'
        elif factor == 'operating_cashoutflow':  # 经营活动现金流出
            return 'STOT_CASH_OUTFLOWS_OPER_ACT'
        elif factor == 'investment_cashinflow':  # 投资活动现金流入
            return 'STOT_CASH_INFLOWS_INV_ACT'
        elif factor == 'investment_cashoutflow':  # 投资活动现金流出
            return 'STOT_CASH_OUTFLOWS_INV_ACT'
        elif factor == 'investment_cashflow':  # 投资活动现金流量净额
            return 'NET_CASH_FLOWS_INV_ACT'
        elif factor == 'finance_cashinflow':  # 筹资活动现金流入
            return 'STOT_CASH_INFLOWS_FNC_ACT'
        elif factor == 'finance_cashoutflow':  # 筹资活动现金流出
            return 'STOT_CASH_OUTFLOWS_FNC_ACT'
        elif factor == 'finance_cashflow':  # 筹资活动现金流量净额
            return 'NET_CASH_FLOWS_FNC_ACT'


    code_str = _coderange2str(code_range)
    min_date, max_date = _date_range_limits(date_range)
    factor_str = _factor2str(factor)

    report_period_start = _report_range_min(min_date)

    sql = "SELECT S_INFO_WINDCODE,ANN_DT,ACTUAL_ANN_DT,REPORT_PERIOD,STATEMENT_TYPE," + factor_str \
          + """ FROM wind.ASHARECASHFLOW
              WHERE STATEMENT_TYPE IN ('408001000','408004000','408005000') 
                    AND S_INFO_WINDCODE IN (%s) AND REPORT_PERIOD >= '%s'
                    AND ACTUAL_ANN_DT < '%s'
              ORDER BY S_INFO_WINDCODE,REPORT_PERIOD,ACTUAL_ANN_DT""" % (
          code_str, report_period_start, max_date)
    columns = ['Code', 'AnnDate', 'ActualAnnDate', 'ReportPeriod', 'StatementType', factor_str]

    data = __db.query_by_SQL("wind",sql)

    data.columns = columns
    data = data.set_index(['Code', 'ReportPeriod', 'ActualAnnDate'])
    data = data.sort_index(level=['ActualAnnDate', 'Code', 'ReportPeriod'])

    factor_port = FactorsPort(code_range)

    for item in data[data.index.get_level_values('ActualAnnDate') < date_range[0]].iterrows():
        # item[0]: code, reportperiod, actualanndate
        factor_port.push(item[0][0], item[0][1], item[0][2], item[1][factor_str])

    # latest_released_report为在指定交易日已知的最新财务报表报告期
    latest_released_report = pd.DataFrame([], columns=code_range, index=date_range)
    # latest_factor_value为最新财务报表的数据
    factor_values = pd.DataFrame([], columns=code_range, index=date_range)
    snapshots = pd.DataFrame([], columns=code_range, index=date_range)

    season_table = pd.DataFrame([], columns=['prevQuarter', 'prevAnnual', 'lastyearQuarter'], \
                                index=_generate_season_table(report_period_start, max_date))

    season_table['prevQuarter'] = season_table.index.map(lambda s: _prev_quarter(s))
    season_table['prevAnnual'] = season_table.index.map(lambda s:_prev_annual(s))
    season_table['lastyearQuarter'] = season_table.index.map(lambda s: _lastyear_quarter(s))

    date_beginning = date_range[0]

    data = data.reset_index().set_index(['ActualAnnDate', 'Code'])
    data = data.sort_index(level=['ActualAnnDate', 'Code'])

    # 产生初始值的公告日
    ann_date_container = data.index.get_level_values('ActualAnnDate').drop_duplicates().values
    ann_date_container = ann_date_container[ann_date_container >= date_beginning]

    func = _find_eff_date(date_range)
    func.send(None)

    # # 以下计算初始值
    for code in latest_released_report.columns:
        if not factor_port.get_latest_report(code):
            continue
        latest_released_report.loc[latest_released_report.index[0], code] = factor_port.get_latest_report(code)
        if factor_method in ('TTM', 'ttm'):
            if latest_released_report.loc[latest_released_report.index[0], code][4:6] == '12':
                factor_values.loc[factor_values.index[0], code] = factor_port.top(code).factor
            else:
                try:
                    factor_values.loc[factor_values.index[0], code] = factor_port.top(code).factor \
                        + factor_port.get(code, season_table.loc[latest_released_report.loc[latest_released_report.index[0], code], 'prevAnnual']).factor \
                        - factor_port.get(code, season_table.loc[latest_released_report.loc[latest_released_report.index[0], code], 'lastyearQuarter']).factor
                except KeyError:
                    factor_values.loc[factor_values.index[0], code] = None
                except TypeError:
                    if factor in []:
                        factor_values.loc[factor_values.index[0], code] = None
                    else:
                        try:
                            latest = factor_port.top(code).factor
                            prevAnnual = factor_port.get(code, season_table.loc[latest_released_report.loc[latest_released_report.index[0], code], 'prevAnnual']).factor
                            lastyearQuarter = factor_port.get(code, season_table.loc[latest_released_report.loc[latest_released_report.index[0], code], 'lastyearQuarter']).factor

                            latest = .0 if latest is None else latest
                            prevAnnual = .0 if prevAnnual is None else prevAnnual
                            lastyearQuarter = .0 if lastyearQuarter is None else lastyearQuarter

                            factor_values.loc[factor_values.index[0], code] = latest + prevAnnual - lastyearQuarter
                        except KeyError:
                            factor_values.loc[factor_values.index[0], code] = None
        snapshots.loc[factor_values.index[0], code] = factor_port.snap(code, factor_port.get_latest_report(code))

    # print('Initialization Finished')

    for ann_date in ann_date_container:
        ipos, eff_date = func.send(ann_date)
        new_info = data.loc[ann_date, ['ReportPeriod', factor_str]]
        for new_factor in new_info.iterrows():
            # 需要更新处理的新信息：code, report_period, ann_date, factor
            code, report_period, factor = new_factor[0], new_factor[1]['ReportPeriod'], new_factor[1][factor_str]
            # 若数据过于陈旧，则本次更新取消
            if report_period < report_period_start:
                continue

            # 新数据推送入库之前的最新报告期
            old_latest_report = factor_port.get_latest_report(code)
            # 新数据推送入库
            factor_port.push(code, report_period, ann_date, factor)
            # 新数据推送入库后的最新报告期
            new_latest_report = factor_port.get_latest_report(code)

            snapshots.loc[eff_date, code] = factor_port.snap(code, factor_port.get_latest_report(code))
            # 至此，存在3个报告期，old_latest_report, report_period, new_latest_report
            # 如果：
            # 情形一：report_period < old_latest_report, 则 report_period < old_latest_report = new_latest_report
            # 情形二：report_period = old_latest_report，则 report_period = old_latest_report = new_latest_report
            # 情形三：report_period > old_latest_report，则 old_latest_report < report_period = new_latest_report
            # 更新factor_values值
            if (old_latest_report) and (report_period <= old_latest_report):
                # 本次推送的数据是对之前报告数据的更新
                assert old_latest_report == new_latest_report, "Error in old_latest_report == new_latest_report"
                if factor_method in ('TTM', 'ttm'):
                    if old_latest_report[4:6] == '12':
                        factor_values.loc[eff_date, code] = factor_port.get(code, old_latest_report).factor
                    else:
                        try:
                            factor_values.loc[eff_date, code] = factor_port.get(code, old_latest_report).factor \
                                + factor_port.get(code, season_table.loc[old_latest_report, 'prevAnnual']).factor \
                                - factor_port.get(code, season_table.loc[old_latest_report, 'lastyearQuarter']).factor
                        except KeyError:
                            factor_values.loc[eff_date, code] = None
                        except TypeError:
                            if factor in []:
                                factor_values.loc[eff_date, code] = None
                            else:
                                try:
                                    latest = factor_port.get(code, new_latest_report).factor
                                    prevAnnual = factor_port.get(code, season_table.loc[new_latest_report, 'prevAnnual']).factor
                                    lastyearQuarter = factor_port.get(code, season_table.loc[new_latest_report, 'lastyearQuarter']).factor

                                    latest = .0 if latest is None else latest
                                    prevAnnual = .0 if prevAnnual is None else prevAnnual
                                    lastyearQuarter = .0 if lastyearQuarter is None else lastyearQuarter

                                    factor_values.loc[eff_date, code] = latest + prevAnnual - lastyearQuarter
                                except KeyError:
                                    factor_values.loc[eff_date, code] = None
            else:
                assert report_period == new_latest_report, "Error in report_period == new_latest_report"
                # 新信息为新发布的财务报告
                latest_released_report.loc[eff_date, code] = new_latest_report
                if factor_method in ('TTM', 'ttm'):
                    if new_latest_report[4:6] == '12':
                        factor_values.loc[eff_date, code] = factor_port.get(code, new_latest_report).factor
                    else:
                        try:
                            factor_values.loc[eff_date, code] = factor_port.get(code, new_latest_report).factor \
                                + factor_port.get(code, season_table.loc[new_latest_report, 'prevAnnual']).factor \
                                - factor_port.get(code, season_table.loc[new_latest_report, 'lastyearQuarter']).factor
                        except KeyError:
                            factor_values.loc[eff_date, code] = None
                        except TypeError:
                            if factor in []:
                                factor_values.loc[eff_date, code] = None
                            else:
                                try:
                                    latest = factor_port.get(code, new_latest_report).factor
                                    prevAnnual = factor_port.get(code, season_table.loc[new_latest_report, 'prevAnnual']).factor
                                    lastyearQuarter = factor_port.get(code, season_table.loc[new_latest_report, 'lastyearQuarter']).factor

                                    latest = .0 if latest is None else latest
                                    prevAnnual = .0 if prevAnnual is None else prevAnnual
                                    lastyearQuarter = .0 if lastyearQuarter is None else lastyearQuarter

                                    factor_values.loc[eff_date, code] = latest + prevAnnual - lastyearQuarter
                                except KeyError:
                                    factor_values.loc[eff_date, code] = None

    latest_released_report = latest_released_report.fillna(method='ffill')
    factor_values = factor_values.fillna(method='ffill')
    snapshots = snapshots.fillna(method='ffill')

    return factor_values, snapshots, latest_released_report, data


# The following part contains functions extracted from make_financial_factor.py
def getTradeCalendar(start_date: str, end_date: str) -> list:
    '''
    获取某一日期区间内的所有交易日（包括起始日期和终止日期）。
    :param start_date: str, 起始日期, "YYYMMDD"
    :param end_date:str, 终止日期, "YYYMMDD"
    :return: list, 交易日列表
    '''
    sql = \
        '''
        SELECT
            TRADE_DAYS 
        FROM
            asharecalendar
        WHERE
            S_INFO_EXCHMARKET = 'SSE' 
            AND trade_days BETWEEN {} AND {}
    '''.format(start_date, end_date)
    tradingDays = __db.query_by_SQL(sql)
    return sorted(tradingDays.TRADE_DAYS.tolist())

def get_all_available_stocks(start_date:str, end_date:str) -> pd.DataFrame:
    '''
    获取某一日起区间内所出现过的所有股票的代码、上市日和退市日
    :param start_date: str, 初始日期, “YYYYMMDD”
    :end_date: str, 结束日期，“YYYYMMDD”
    :return: pd.DataFrame, columns = [S_INFO_WINDCODE -]
    '''
    sql = \
        '''
        SELECT
            S_INFO_WINDCODE, S_INFO_LISTDATE, S_INFO_DELISTDATE
        FROM
            wind.ASHAREDESCRIPTION
        WHERE
            S_INFO_LISTDATE <= {1} AND S_INFO_LISTDATE <= '20190731'
            AND (S_INFO_DELISTDATE >= {0} OR S_INFO_DELISTDATE IS NULL)
        '''.format(start_date, end_date)
    all_available_stocks = __db.query_by_SQL('wind', sql)
    all_available_stocks = all_available_stocks[all_available_stocks["s_info_windcode"].apply(
        lambda s: s[0] in "036T")]

    return all_available_stocks

def fill_out_na(snapshots):
    for key, value in snapshots.items():
        if pd.isna(value):
            snapshots[key] = 0
    return snapshots

def make_financial_factor(start_date, end_date, factor, test_mode=False):
    pd.set_option("display.max_columns", None)
    all_available_stocks = get_all_available_stocks(start_date, end_date)
    all_available_stocks["end_dt"] = np.where(all_available_stocks["s_info_delistdate"].isna(), end_date,
                                              all_available_stocks["s_info_delistdate"])
    all_available_stocks["start_dt"] = all_available_stocks["s_info_listdate"].apply(
        lambda date: start_date if date < start_date else date)
    all_available_stocks = all_available_stocks[all_available_stocks["s_info_windcode"] != "600087.SH"]
    if test_mode is True:
        all_available_stocks = all_available_stocks[:50]
    i = 0
    factor_df = pd.DataFrame(columns=[factor, "ticker"])
    if factor in ["net_income", "total_revenue",
                "revenue", "total_opcost", "operating_cost", "sale_expense",
                "management_expense", "research_expense", "financial_expense", "operating_profit"]:
        dataPort = incomeRaw
        factor_method = "ttm"
    elif factor in ['total_assets', 'total_equities_exc_min', 'total_equities_inc_min',
                    'noncur_liabilities', 'total_liabilities',
                    'longterm_loan', 'bonds_payable', 'longterm_payable', 'preferred_stock',
                    "cash", "tradable_financialasset", "notes_receiveable", "accounts_receivable",
                    "inventory", "fixed_asset", "construction_inprogress", "intangible_asset",
                    "development_expenditure", "goodwill", "notes_payable", "accounts_payable"]:
        dataPort = balanceRaw
        factor_method = "latest"
    elif factor in ["operating_cashflow",
                    "operating_cashinflow", "operating_cashoutflow", "investment_cashinflow", "investment_cashoutflow",
                    "investment_cashflow", "finance_cashinflow", "finance_cashoutflow", "finance_cashflow"]:
        dataPort = cashflowRaw
        factor_method = "ttm"


    for _, record in all_available_stocks.iterrows():
        try:
            if record["s_info_windcode"] == "000498.SZ":
                continue
            codes = [record["s_info_windcode"], ]
            date_range = calendar(record["start_dt"], record["end_dt"])
            factor_values, snapshots, _, _ = dataPort(codes, date_range, factor, factor_method=factor_method)

            df = pd.DataFrame(index=factor_values.index)
            df[factor] = factor_values.iloc[:, 0]
            df[factor + "_snapshots"] = snapshots.iloc[:, 0]
            df["ticker"] = codes[0]
            factor_df = factor_df.append(df)
            i += 1
            print('\rMaking financial factor: ' + factor + "; count: " +  str(i), end= " ")
        except Exception as e:
            print(codes)
            print(date_range)
            df.to_csv("debug.csv")
            raise (e)


    factor_df["tradeday"] = factor_df.index
    factor_df.index = range(factor_df.shape[0])
    factor_df = factor_df[["tradeday", "ticker", factor, factor + "_snapshots"]]
    if factor in ["longterm_loan", "bonds_payable", "longterm_payable", "preferred_stock",
                  "cash", "tradable_financialasset", "notes_receiveable", "accounts_receivable",
                  "inventory", "fixed_asset", "construction_inprogress", "intangible_asset",
                  "development_expenditure", "goodwill", "notes_payable", "accounts_payable",
                  "revenue", "total_opcost", "operating_cost", "sale_expense",
                  "management_expense", "research_expense", "financial_expense", "operating_profit",
                  "operating_cashinflow", "operating_cashoutflow", "investment_cashinflow", "investment_cashoutflow",
                  "investment_cashflow", "finance_cashinflow", "finance_cashoutflow", "finance_cashflow"]:
        # In this circumstance, None or NaN mean not missing but 0 in the financial statements
        print("fill out NaN and None")
        factor_df[factor] = factor_df[factor].apply(lambda x: 0 if pd.isna(x) else x)
        factor_df[factor + "_snapshots"] = factor_df[factor + "_snapshots"].apply(lambda s: "{}" if pd.isnull(s) else s)
        factor_df[factor + "_snapshots"] = factor_df[factor + "_snapshots"].apply(lambda s: s.replace("None", '0'))
    factor_df.loc[factor_df[factor].isnull(), factor] = None
    return factor_df


def get_listed_stocks():
    sql = '''
        SELECT
            S_INFO_WINDCODE 
        FROM
            wind.ASHAREDESCRIPTION 
        WHERE
            S_INFO_DELISTDATE IS NULL 
            AND substr( S_INFO_WINDCODE, 1, 1 ) != 'A'
        '''
    stock_list_df = __db.query_by_SQL("wind", sql)
    print(stock_list_df)

def report_period_generator(period):
    date = dt.datetime.now().date()
    while period > 0:
        if date.month in (1, 2, 3):
            date = date.replace(year=date.year - 1, month=12, day=31)
        elif date.month in (4, 5, 6):
            date = date.replace(year=date.year, month=3, day=31)
        elif date.month in (7, 8, 9):
            date = date.replace(year=date.year, month=6, day=30)
        elif date.month in (10, 11, 12):
            date = date.replace(year=date.year, month=9, day=30)
        period -= 1
        yield dt.datetime.strftime(date, "%Y%m%d")
