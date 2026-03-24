# MQS Trading System - Main Backtest Module
"""
How to run a backtest:
1. Load your portfolio through imports such as:
    `from portfolios.portfolio_n.strategy import StrategyClass`.
2. Setup main args with start date, end date, initial capital, and slippage.
3. Add class to classes list comment out unused strategies for faster testing.
4. Run python -m src.main_backtest
"""

import logging
import warnings
from copy import deepcopy

import pandas as pd

from src.backtest.backtest_engine import BacktestEngine
from src.common.database.MQSDBConnector import MQSDBConnector
from src.portfolios.portfolio_1.strategy import VolMomentum
from src.portfolios.portfolio_2.strategy import MomentumStrategy
from src.portfolios.portfolio_3.strategy import RegimeAdaptiveStrategy
from src.portfolios.portfolio_4.strategy import TrendRotateStrategy
from src.portfolios.portfolio_dummy.strategy import CrossoverRmiStrategy

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)


DEFAULT_PORTFOLIO_CLASSES = [
    #  VolMomentum,
    # MomentumStrategy,
    # RegimeAdaptiveStrategy,
    TrendRotateStrategy,
    # CrossoverRmiStrategy,
]

AVAILABLE_PORTFOLIO_CLASSES = [
    VolMomentum,
    MomentumStrategy,
    RegimeAdaptiveStrategy,
    TrendRotateStrategy,
    CrossoverRmiStrategy,
]

# Adapters for external vectorized backtest approximations of the above strategies.
"""
quick: True/False flag to control whether the adapter should use faster approximations (e.g. fewer lookback periods, simpler logic) to speed up execution during fast-mode backtests. Adapters can choose how to interpret this flag based on the complexity of the underlying strategy and the desired tradeoff between accuracy and speed.

mc_enabled: True/False flag to control whether the adapter should prepare its output in a way that is compatible with Monte Carlo simulation. This may involve ensuring that the target_weights and signal_strength outputs are structured appropriately for resampling and path generation during the Monte Carlo process. Adapters can choose how to interpret this flag based on the specific requirements of their strategy and the intended use of the Monte Carlo results.

mc_n_sims: Integer specifying the number of Monte Carlo simulation paths that will be generated during fast-mode backtests when mc_enabled is True. Adapters can use this information to optimize their output preparation, such as by precomputing certain values or structuring their outputs in a way that facilitates efficient resampling and path generation for the specified number of simulations.

mc_method: String specifying the method of Monte Carlo simulation (e.g. "bootstrap", "parametric") that will be used during fast-mode backtests when mc_enabled is True. Adapters can use this information to tailor their output preparation to the specific requirements of the chosen Monte Carlo method, such as by ensuring that their outputs are compatible with the resampling techniques or distributional assumptions associated with the specified method.

mc_block_size: Integer specifying the block size to be used for block bootstrapping during Monte Carlo simulation when mc_enabled is True and mc_method is "bootstrap". Adapters can use this information to structure their outputs in a way that facilitates efficient block resampling during the Monte Carlo process, such as by organizing their outputs into contiguous blocks of the specified size.

mc_seed: Integer seed value for random number generation during Monte Carlo simulation when mc_enabled is True. Adapters can use this information to ensure that any stochastic elements of their output preparation are reproducible and consistent across different runs of the backtest, which can be important for debugging and result validation purposes.

mc_plot_percentiles: List of integers specifying the percentiles to be plotted during Monte Carlo simulation when mc_enabled is True. Adapters can use this information to prepare their outputs in a way that facilitates the generation of percentile plots during the Monte Carlo process, such as by structuring their outputs to allow for easy calculation and visualization of the specified percentiles.
"""

FAST_MODE_CONFIG = {
    "years_back": 3,
    "benchmark_label": "SPY",
    "quick": True,
    "mc_enabled": True,
    "mc_n_sims": 10000,
    "mc_method": "bootstrap",
    "mc_block_size": 5,  # Preserve short-term autocorrelation for block bootstrap.
    "mc_seed": None,
    "mc_plot_percentiles": [10, 50, 90],
}


def _resolve_fast_mode_config(
    fast_config=None,
    *,
    fast_years_back=None,
    fast_benchmark_label=None,
):
    """Resolve fast-mode config; when both are provided, fast_config values take precedence."""
    resolved = deepcopy(FAST_MODE_CONFIG)

    if fast_years_back is not None:
        resolved["years_back"] = int(fast_years_back)
    if fast_benchmark_label is not None:
        resolved["benchmark_label"] = str(fast_benchmark_label)

    if fast_config:
        if (
            "years_back" in fast_config
            and fast_years_back is not None
            and int(fast_config["years_back"]) != int(fast_years_back)
        ):
            warnings.warn(
                "fast_config['years_back'] overrides fast_years_back.",
                UserWarning,
                stacklevel=2,
            )
        if (
            "benchmark_label" in fast_config
            and fast_benchmark_label is not None
            and str(fast_config["benchmark_label"]) != str(fast_benchmark_label)
        ):
            warnings.warn(
                "fast_config['benchmark_label'] overrides fast_benchmark_label.",
                UserWarning,
                stacklevel=2,
            )
        resolved.update(dict(fast_config))

    return resolved


def main(
    portfolio_classes=None,
    start_date="2024-01-01",
    end_date="2025-09-01",
    initial_capital=1000000.0,
    slippage=0,
    backtest_mode="fast",
    fast_config=None,
    fast_years_back=None,
    fast_benchmark_label=None,
):
    """
    Main entry point for the MQS Trading System backtests.
    comment/uncomment the classes in the portfolio_classes list to run different strategies.
    """
    # Strategy selection (user-editable):
    # - If portfolio_classes is explicitly passed to main(), it is used as-is.
    # - Otherwise the default list below is used.
    if portfolio_classes is None:
        selected_portfolios = list(DEFAULT_PORTFOLIO_CLASSES)
    else:
        selected_portfolios = list(portfolio_classes)
        if len(selected_portfolios) == 0:
            raise ValueError(
                "portfolio_classes cannot be empty; pass None to use DEFAULT_PORTFOLIO_CLASSES."
            )

    # Fast-mode tuning (single source of truth):
    # - Legacy fast_years_back / fast_benchmark_label values are still accepted.
    # - Any provided fast_config keys override defaults and legacy values.
    resolved_fast_config = _resolve_fast_mode_config(
        fast_config=fast_config,
        fast_years_back=fast_years_back,
        fast_benchmark_label=fast_benchmark_label,
    )

    if end_date is None:
        end_date = pd.Timestamp.now(tz="UTC").normalize().strftime("%Y-%m-%d")

    if backtest_mode is None:
        warnings.warn(
            "currently using 'event'. Set `backtest_mode` explicitly to silence this warning.",
            FutureWarning,
            stacklevel=2,
        )
        backtest_mode = "event"
    else:
        backtest_mode = str(backtest_mode).lower()

    # Runtime execution bootstrap (normally no edits needed below this line).
    # ======================================================
    try:
        dbconn = MQSDBConnector()

        backtest_engine = BacktestEngine(db_connector=dbconn, backtest_executor=None)

        backtest_engine.setup(
            portfolio_classes=selected_portfolios,
            start_date=start_date,
            end_date=end_date,
            initial_capital=initial_capital,
            slippage=slippage,  # 0.1 basis point
            backtest_mode=backtest_mode,
            fast_config=resolved_fast_config,
        )

        backtest_engine.run()
        return backtest_engine

    finally:
        logging.info("===== DONE =====")


if __name__ == "__main__":
    main()
