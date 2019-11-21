from common import db as __db
from datetime import datetime as __datetime
from dateutil.relativedelta import relativedelta as __relativedelta
from calendar import monthrange as __monthrange



def get_suspend_list(date):
    sus = __db.query_MongoDB('factor', 'universedb', 'universe', {'trade_date':date, 'suspend':True}, {'code':1, '_id':0})
    return [] if sus.shape[1]==0 else list(sus['code'])

def nearby_season_month(datestr, offset):
    date = __datetime.strptime(datestr, '%Y%m%d')
    date = date.replace(day=1)
    season_month = date.replace(month=3 * (date.month // 3 + int(date.month % 3 > 0))) + __relativedelta(months=3 * offset)
    return season_month.replace(day=__monthrange(season_month.year, season_month.month)[1]).strftime('%Y%m%d')



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
    return stock_list_df


def report_period_generator(period, date=None):
    if date is None:
        date = __datetime.now().date()
    else:
        date = __datetime.strptime(date, "%Y%m%d").date()
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
        yield __datetime.strftime(date, "%Y%m%d")


def Quarters2LastDec(date):
    date = __datetime.strptime(date, "%Y%m%d")
    if date.month in (1, 2, 3):
        return 1
    elif date.month in (4, 5, 6):
        return 2
    elif date.month in (7, 8, 9):
        return 3
    else:
        return 4



