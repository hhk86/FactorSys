'''
工具包，提供并行计算、统计计算等封装方法
'''
from utils.statistics_util import *
from utils.data_util import *
from dask import delayed as __delayed, compute as __compute
import numpy as __np
import pandas as __pd



def list_convert_tuple_str(l,encoding='UTF-8'):
    if not isinstance(l, list):
        raise Exception(u'错误的参数')
    return "('%s')" % l[0] if len(l) == 1 else str(tuple(l))

def loading_data_by_split_code_list(func,code_list,window=1000):
    values = map(lambda codes: __delayed(func)(codes), map(lambda x: code_list[x:x + window], range(0, len(code_list), window)))
    return __pd.concat(__compute(*values, scheduler="threading"))

def rolling_apply_on_dataframe_index(func, df, window,step=1, *args):
    values = map(lambda x: __delayed(func)(x, *args), map(lambda x: df.iloc[x - window:x], range(df.shape[0], window - 1, -step)))
    return __compute(*values, scheduler="multiprocessing")

def apply_on_dataframe_columns(func, df, *args):
    values = map(lambda x: __delayed(func)(x, *args), map(lambda x: df[x], df.columns))
    return __compute(*values, scheduler="multiprocessing")

def apply_on_dataframes(func,df_list,*args):
    values = map(lambda x: __delayed(func)(x, *args), df_list)
    return __compute(*values, scheduler="multiprocessing")

def apply_on_factors(func,factors,*args):
    values = map(lambda x: __delayed(func)(x, *args), factors)
    return __compute(*values, scheduler="multiprocessing")