import datetime
import operators

if __name__ == '__main__':
    # date = datetime.datetime.now().strftime('%Y%m%d')
    # date = '20190815'
    # operators.execute('barra_size_operator.BarraSizeOperator') # , date)
    # operators.execute('universe.Universe')
    # operators.execute('barra_beta_operator.BarraBetaOperator')
    # operators.execute('barra_momentum_operator.BarraMomentumOperator', date)
    # operators.execute('barra_non_linear_size_operator.BarraNonLinearSizeOperator', date)
    # operators.execute('barra_residual_volatility_operator.BarraResidualVolatilityOperator', date)

    from operators.pre_basic_balance_operator import PreBasicBalanceOperator
    PreBasicBalanceOperator.execute('20191101')

    from operators.pre_basic_income_operator import PreBasicIncomeOperator
    PreBasicIncomeOperator.execute('20191101')

    from operators.pre_basic_cashflow_operator import PreBasicCashflowOperator
    PreBasicCashflowOperator.execute('20191101')