from operators.operator import Operator
import numpy as np
from dateutil.parser import parse
import datetime as dt


class PreUniverse(Operator):
    schema = 'universedb'
    table = 'universe'

    # @classmethod
    # def init_date_list(cls):
    #     return ['20190801', '20190802']

    @classmethod
    def load_data(cls, date, code_list=None):
        print(date)
        sql = \
            '''
            SELECT
                '{0}' AS trade_date,
                S_INFO_WINDCODE AS code
            FROM
                wind.ASHAREDESCRIPTION 
            WHERE
                S_INFO_LISTDATE <= '{0}'
                AND (
                    S_INFO_DELISTDATE > '{0}'
                OR S_INFO_DELISTDATE IS NULL 
                )
            '''.format(date)
        data = cls.db.query_by_SQL('wind', sql)
        data = cls.make_name(date, data)
        data = cls.make_citics_industry(date, data)
        data = cls.make_sw_industries(date, data)
        data = cls.make_suspend(date, data)
        data = cls.make_st(date, data)
        return data

    @classmethod
    def make_name(cls, tradeday, daily_universe):
        tradeday = parse(tradeday).strftime('%Y%m%d')
        sql = \
            '''
            SELECT
                S_INFO_WINDCODE,
                S_INFO_NAME,
                BEGINDATE,
                ENDDATE 
            FROM
                wind.ASHAREPREVIOUSNAME 
            WHERE
                BEGINDATE <= {0}
                AND (
                ENDDATE >= {0} 
                OR ENDDATE IS NULL)
            '''.format(tradeday)

        name_range = cls.db.query_by_SQL('wind', sql)
        name_range['enddate'] = np.where(name_range['enddate'].isna(), tradeday, name_range['enddate'])

        daily_universe['name'] = None

        for idx, record in name_range.iterrows():
            ticker = record['s_info_windcode']
            start_dt = record['begindate']
            end_dt = record['enddate']
            name = record['s_info_name']
            daily_universe['name'] = np.where((daily_universe['code'] == ticker) \
                                              & (daily_universe['trade_date'] >= start_dt) \
                                              & (daily_universe['trade_date'] <= end_dt),
                                              name, daily_universe['name'])
        daily_universe.index = list(range(daily_universe.shape[0]))
        return daily_universe

    @classmethod
    def make_citics_industry(cls, trade_date, daily_universe):
        sql1 = \
            '''
            SELECT
                a.S_INFO_WINDCODE,
                b.INDUSTRIESNAME,
                a.ENTRY_DT,
                a.REMOVE_DT 
            FROM
                wind.ASHAREINDUSTRIESCLASSCITICS a,
                wind.ASHAREINDUSTRIESCODE b 
            WHERE
                substr( a.CITICS_IND_CODE, 1, 4 ) = substr( b.INDUSTRIESCODE, 1, 4 ) 
                AND b.LEVELNUM = '2' 
                AND ENTRY_DT <= {0}
                AND (REMOVE_DT >= {0} OR REMOVE_DT IS NULL)
            '''.format(trade_date)
        industry_L1 = cls.db.query_by_SQL('wind', sql1)

        sql2 = \
            '''
            SELECT
                a.S_INFO_WINDCODE,
                b.INDUSTRIESNAME,
                a.ENTRY_DT,
                a.REMOVE_DT 
            FROM
                wind.ASHAREINDUSTRIESCLASSCITICS a,
                wind.ASHAREINDUSTRIESCODE b 
            WHERE
                substr( a.CITICS_IND_CODE, 1, 6 ) = substr( b.INDUSTRIESCODE, 1, 6 ) 
                AND b.LEVELNUM = '3' 
                AND ENTRY_DT <= {0}
                AND (REMOVE_DT >= {0} OR REMOVE_DT IS NULL)
            '''.format(trade_date)
        industry_L2 = cls.db.query_by_SQL('wind', sql2)

        industry_L1['remove_dt'] = np.where(industry_L1['remove_dt'].isna(), trade_date, industry_L1['remove_dt'])
        industry_L2['remove_dt'] = np.where(industry_L2['remove_dt'].isna(), trade_date, industry_L2['remove_dt'])

        daily_universe['citic_industry_L1'] = None
        daily_universe['citic_industry_L2'] = None

        for idx, record in industry_L1.iterrows():
            ticker = record['s_info_windcode']
            start_dt = record['entry_dt']
            end_dt = record['remove_dt']
            industry_name = record['industriesname']
            daily_universe['citic_industry_L1'] = np.where((daily_universe['code'] == ticker) \
                                                           & (daily_universe['trade_date'] >= start_dt) \
                                                           & (daily_universe['trade_date'] <= end_dt),
                                                           industry_name, daily_universe['citic_industry_L1'])
        for idx, record in industry_L2.iterrows():
            ticker = record['s_info_windcode']
            start_dt = record['entry_dt']
            end_dt = record['remove_dt']
            industry_name = record['industriesname']
            daily_universe['citic_industry_L2'] = np.where((daily_universe['code'] == ticker) \
                                                           & (daily_universe['trade_date'] >= start_dt) \
                                                           & (daily_universe['trade_date'] <= end_dt),
                                                           industry_name, daily_universe['citic_industry_L2'])

        daily_universe.index = list(range(daily_universe.shape[0]))
        return daily_universe

    @classmethod
    def make_sw_industries(cls, trade_date, daily_universe):
        sql1 = \
            '''
            SELECT
                a.S_INFO_WINDCODE,
                b.INDUSTRIESNAME,
                a.ENTRY_DT,
                a.REMOVE_DT 
            FROM
                wind.ASHARESWINDUSTRIESCLASS a,
                wind.ASHAREINDUSTRIESCODE b 
            WHERE
                substr( a.SW_IND_CODE, 1, 4 ) = substr( b.INDUSTRIESCODE, 1, 4 ) 
                AND b.LEVELNUM = '2' 
                AND ENTRY_DT <= {0}
                AND (REMOVE_DT >= {0} OR REMOVE_DT IS NULL)
            '''.format(trade_date)
        industry_L1 = cls.db.query_by_SQL('wind', sql1)

        sql2 = \
            '''
            SELECT
                a.S_INFO_WINDCODE,
                b.INDUSTRIESNAME,
                a.ENTRY_DT,
                a.REMOVE_DT 
            FROM
                wind.ASHARESWINDUSTRIESCLASS a,
                wind.ASHAREINDUSTRIESCODE b 
            WHERE
                substr( a.SW_IND_CODE, 1, 6 ) = substr( b.INDUSTRIESCODE, 1, 6 ) 
                AND b.LEVELNUM = '3' 
                AND ENTRY_DT <= {0}
                AND (REMOVE_DT >= {0} OR REMOVE_DT IS NULL)
            '''.format(trade_date)
        industry_L2 = cls.db.query_by_SQL('wind', sql2)

        industry_L1['remove_dt'] = np.where(industry_L1['remove_dt'].isna(), trade_date, industry_L1['remove_dt'])
        industry_L2['remove_dt'] = np.where(industry_L2['remove_dt'].isna(), trade_date, industry_L2['remove_dt'])

        daily_universe['sw_industry_L1'] = None
        daily_universe['sw_industry_L2'] = None

        for idx, record in industry_L1.iterrows():
            ticker = record['s_info_windcode']
            start_dt = record['entry_dt']
            end_dt = record['remove_dt']
            industry_name = record['industriesname']
            daily_universe['sw_industry_L1'] = np.where((daily_universe['code'] == ticker) \
                                                        & (daily_universe['trade_date'] >= start_dt) \
                                                        & (daily_universe['trade_date'] <= end_dt),
                                                        industry_name, daily_universe['sw_industry_L1'])
        for idx, record in industry_L2.iterrows():
            ticker = record['s_info_windcode']
            start_dt = record['entry_dt']
            end_dt = record['remove_dt']
            industry_name = record['industriesname']
            daily_universe['sw_industry_L2'] = np.where((daily_universe['code'] == ticker) \
                                                        & (daily_universe['trade_date'] >= start_dt) \
                                                        & (daily_universe['trade_date'] <= end_dt),
                                                        industry_name, daily_universe['sw_industry_L2'])
        daily_universe.index = list(range(daily_universe.shape[0]))
        return daily_universe

    @classmethod
    def make_st(cls, trade_date, daily_universe):
        def parse_ST_type(s: str) -> str:
            '''
            将特别处理类型由字母解析成文字
            :param s: str
            :return: str, 共6种特别处理类型
            '''
            if s == 'S':
                return "特别处理(ST)"
            elif s == 'Z':
                return "暂停上市"
            elif s == 'P':
                return "特别转让服务(PT)"
            elif s == 'L':
                return "退市处理"
            elif s == 'X':
                return "创业板暂停上市风险警示"
            elif s == 'T':
                return "退市"
            else:
                raise ValueError("特别处理类型错误：" + s)

        sql = \
            '''
            SELECT
                S_INFO_WINDCODE,
                S_TYPE_ST,
                ENTRY_DT,
                REMOVE_DT 
            FROM
                wind.ASHAREST
            WHERE
                ENTRY_DT <= {0}
                AND (
                REMOVE_DT > {0}   --由于[ENTRY_DT, REMOVE_DT）左闭右开, 此处不能取等号
                OR REMOVE_DT IS NULL)
            '''.format(trade_date)

        st = cls.db.query_by_SQL('wind', sql)

        st["ST_type"] = st["s_type_st"].apply(parse_ST_type)

        # 由于[ENTRY_DT, REMOVE_DT）左闭右开，不能用end_date来代替Null的REMOVE_DT, 用2099年12月31日代替右端点
        st['remove_dt'] = np.where(st['remove_dt'].isna(), "20991231", st['remove_dt'])

        daily_universe['ST'] = False
        daily_universe['ST_type'] = None

        for idx, record in st.iterrows():
            ticker = record['s_info_windcode']
            start_dt = record['entry_dt']
            end_dt = record['remove_dt']
            ST_type = record['ST_type']
            logic = (daily_universe['code'] == ticker) & (daily_universe['trade_date'] >= start_dt) \
                    & (daily_universe['trade_date'] < end_dt)  # 第三个条件没有等号
            daily_universe['ST'] = np.where(logic, True, daily_universe['ST'])
            daily_universe['ST_type'] = np.where(logic, ST_type, daily_universe['ST_type'])
        daily_universe.index = list(range(daily_universe.shape[0]))
        return daily_universe

    @classmethod
    def make_suspend(cls, trade_date, daily_universe):
        def get_last_suspend_list(resump_series) -> list:
            '''

            根据复牌日系列，返回最后一个停牌日列表，最后一个停牌日定义为复牌日的上一个交易日。
            使用最后一个停牌日，使得停牌区间可以用[suspend_date, last_suspend_date]闭区间表示，与单停牌日[suspend_date, suspend_date]
            以及其他函数的闭区间保持一致。
            :param resump_date: str, "YYYYMMDD"
            :return: str, "YYYYMMDD"
            '''
            last_suspend_list = list()
            for resump_date in resump_series:
                if resump_date is None:
                    last_suspend_list.append(None)
                else:
                    last_suspend_list.append(
                        dt.datetime.strftime(dt.datetime.strptime(resump_date, "%Y%m%d") - dt.timedelta(1), "%Y%m%d"))
            return last_suspend_list

        def parse_suspend_type(code: np.int64) -> str:
            '''
            将停牌类型代码解析成文字。
            :param code: int, 9位数字
            :return: str, 共六种停牌类型
            '''

            if code == 444001000:
                return "上午停牌"
            elif code == 444002000:
                return "下午停牌"
            elif code == 444003000:
                return "今起停牌"
            elif code == 444004000:
                return "盘中停牌"
            elif code == 444007000:
                return "停牌1小时"
            elif code == 444016000:
                return "停牌一天"
            else:
                raise ValueError("停牌类型代码错误: " + str(code))

        sql = \
            '''
            SELECT
                S_INFO_WINDCODE,
                S_DQ_SUSPENDDATE,
                S_DQ_RESUMPDATE,
                S_DQ_SUSPENDTYPE,
                S_DQ_CHANGEREASONTYPE,
                S_DQ_CHANGEREASON 
            FROM
                wind.ASHARETRADINGSUSPENSION 
            WHERE
                    ( S_DQ_RESUMPDATE IS NULL 
                    AND S_DQ_SUSPENDDATE >= {0}
                    AND S_DQ_SUSPENDDATE <= {0} ) 
                OR 
                    (S_DQ_RESUMPDATE IS NOT NULL 
                    AND S_DQ_SUSPENDDATE <= {0}
                    AND S_DQ_RESUMPDATE >= {0} )
            '''.format(trade_date)

        name_range = cls.db.query_by_SQL('wind', sql)

        name_range['last_suspend_date'] = np.where(name_range['s_dq_resumpdate'].isna(), name_range["s_dq_suspenddate"],
                                                   get_last_suspend_list(name_range['s_dq_resumpdate']))
        name_range["s_dq_suspendtype"] = name_range["s_dq_suspendtype"].apply(parse_suspend_type)

        daily_universe['suspend'] = False
        daily_universe['suspend_type'] = None
        daily_universe['suspend_reason_code'] = None
        daily_universe['suspend_reason'] = None

        for idx, record in name_range.iterrows():
            ticker = record['s_info_windcode']
            start_dt = record['s_dq_suspenddate']
            end_dt = record['last_suspend_date']
            suspend_type = record['s_dq_suspendtype']
            suspend_reason = record["s_dq_changereason"]
            suspend_reason_code = record["s_dq_changereasontype"]
            logic = (daily_universe['code'] == ticker) & (daily_universe['trade_date'] >= start_dt) & \
                    (daily_universe['trade_date'] <= end_dt)
            daily_universe['suspend'] = np.where(logic, True, daily_universe['suspend'])
            daily_universe['suspend_type'] = np.where(logic, suspend_type, daily_universe['suspend_type'])
            daily_universe['suspend_reason'] = np.where(logic, suspend_reason, daily_universe['suspend_reason'])
            daily_universe['suspend_reason_code'] = np.where(logic, suspend_reason_code,
                                                             daily_universe['suspend_reason_code'])

        daily_universe.index = list(range(daily_universe.shape[0]))
        return daily_universe

    @classmethod
    def fit(cls, datas):
        # do nothing
        return datas

    @classmethod
    def dump_data(cls, datas):
        from_date = datas['trade_date'].min()
        to_date = datas['trade_date'].max()

        delete_criteria = {'trade_date': {'$gte': from_date,'$lte': to_date}}
        cls.db.delete_from_MongoDB(cls.dbname, cls.schema, cls.table, delete_criteria)
        cls.db.insert_to_MongoDB(cls.dbname, cls.schema, cls.table, datas)
