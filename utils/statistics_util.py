import numpy as __np
import statsmodels.api as __sm
from numba import njit as __njit

def beta_wls(x, y, weight):
    # 单因子加权线性回归，计算beta系数
    X = __sm.add_constant(x).values
    W = __np.diag(weight)
    params = __np.linalg.inv(X.T.dot(W).dot(X)).dot(X.T).dot(W).dot(y)
    return params[0], params[1]


def wls(x, y, weight):
    # 加权最小二乘解析解求法
    w = __np.diag(weight)
    c1 = __np.linalg.inv(__np.dot(__np.dot(__np.transpose(x), w), x))
    c2 = __np.dot(__np.dot(__np.transpose(x), w), y)
    return __np.dot(c1, c2)


@__njit()
def exp_weight(half, window):
    # 获取指定半衰期及向量长度的指数衰减权重向量
    alpha = 1-__np.exp(__np.log(0.5) / half)
    x = __np.empty(window, dtype=__np.float64)
    for i in __np.arange(window):
        x[i] = __np.power(1-alpha, window-i-1)
    return x/x.sum()


@__njit()
def exp_weight_cov(mat, half_life, min_window=63,diagonal=False):
    N, K = mat.shape
    result = __np.zeros((K, K), dtype=__np.float64)
    mask = (~__np.isnan(mat)).view(__np.uint8)
    for xi in range(K):
        iterlist = range(xi + 1)
        if diagonal:
            iterlist = range(xi, xi + 1)
        for yi in iterlist:
            com = (mask[:, xi] > 0) & (mask[:, yi] > 0)
            nobs = int(__np.sum(com))
            if nobs < min_window:
                result[xi, yi] = result[yi, xi] = __np.nan
            else:
                x_tmp,y_tmp = mat[:,xi][com],mat[:,yi][com]
                weights = exp_weight(half_life,nobs)
                sum_weight = weights.sum()
                meanx = (x_tmp*weights).sum()/sum_weight
                meany = (y_tmp*weights).sum()/sum_weight
                x_tmp = x_tmp-meanx
                y_tmp = y_tmp-meany
                sum_weight = sum_weight-(weights*weights).sum()/sum_weight
                cov = (x_tmp*y_tmp*weights).sum()/sum_weight
                result[xi, yi] = result[yi, xi] = cov
    return result


def median_absolute_deviation(data, n=5):
    # MAD去极值方法
    med = __np.median(data)
    mad = __np.median(__np.absolute(data - med))
    return __np.clip(data, med - mad * n, med + mad * n)


def sigma_deviation(data, n=3):
    # 标准差去极值方法
    sigma = __np.std(data)
    mean = __np.mean(data)
    return __np.clip(data, mean - sigma * n, mean + sigma * n)


def percent_deviation(data, l_percent=2.5, r_percent=2.5):
    # 百分位去极值方法
    outs = __np.percentile(data, [l_percent, 100-r_percent])
    return __np.clip(data, *outs)


def z_score(data):
    # 均值方差法进行标准化
    return (data-__np.mean(data))/__np.std(data)


def max_min_score(data):
    # 基于数据排序进行标准化
    mx = __np.max(data)
    mn = __np.min(data)
    return (data-mn)/(mx-mn)

