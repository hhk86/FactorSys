import operators
import models
import utils
import services

if __name__ == '__main__':

    # 计算pre级operator
    utils.apply_on_factors(operators.execute,
                           ['pre_risk_free_rate.PreRiskFreeRate',
                            'pre_capital.PreCapital',
                            'pre_universe.PreUniverse'])

    # 计算operator
    utils.apply_on_factors(operators.execute,
                           ['barra_size_operator.BarraSizeOperator',
                            'barra_beta_operator.BarraBetaOperator',
                            'barra_momentum_operator.BarraMomentumOperator',
                            'barra_book_to_price_operator.BarraBookToPriceOperator',
                            'barra_liquidity_operator.BarraLiquidityOperator',
                            'barra_non_linear_size_operator.BarraNonLinearSizeOperator',
                            'barra_earnings_yield_operator.BarraEarningsYieldOperator',
                            'barra_growth_operator.BarraGrowthOperator',
                            'barra_leverage_operator.BarraLeverageOperator',
                            'barra_residual_volatility_operator.BarraResidualVolatilityOperator'])
    # 计算post级operator
    operators.execute('post_barra_factors_stand.PostBarraFactorsStand')

    # 计算model
    models.execute('barra_cne5_model.BarraCNE5Model')

    # 计算service
    services.execute('barra_cne5_service.BarraCNE5RetService')