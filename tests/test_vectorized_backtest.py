from pathlib import Path

import numpy as np
import pandas as pd
import pytest

from src.backtest.vectorized_backtest import VectorBacktester


def _build_price_data(start="2022-01-01", end="2025-12-31", seed=7):
    rng = np.random.default_rng(seed)
    idx = pd.date_range(start=start, end=end, freq="B")
    drift = 0.0003
    noise = rng.normal(0, 0.01, len(idx))
    close = 100 * np.exp(np.cumsum(drift + noise))

    benchmark_noise = rng.normal(0, 0.008, len(idx))
    bench = 100 * np.exp(np.cumsum(0.0002 + benchmark_noise))

    return pd.DataFrame({"close": close, "bench": bench}, index=idx)


def test_run_same_window_previous_years_returns_overlay_and_metrics():
    data = _build_price_data()
    bt = VectorBacktester(data=data)

    seasonal = bt.run_same_window_previous_years(
        start_date="2025-01-02",
        end_date="2025-04-30",
        years_back=2,
        fast_window=5,
        slow_window=10,
        z_entry=1.5,
        z_exit=0.25,
        highlight_years=[2024],
    )

    assert not seasonal.per_year_metrics.empty
    assert set(["year", "total_return", "sharpe"]).issubset(
        seasonal.per_year_metrics.columns
    )

    assert not seasonal.overlay_data.empty
    assert set(
        ["day", "year", "strategy_return", "benchmark_return", "is_highlight"]
    ).issubset(seasonal.overlay_data.columns)
    assert seasonal.overlay_data["is_highlight"].any()


def test_run_same_window_previous_years_supports_configurable_benchmark_column():
    data = _build_price_data(seed=17)
    bt = VectorBacktester(data=data)

    seasonal = bt.run_same_window_previous_years(
        start_date="2025-02-03",
        end_date="2025-05-15",
        years_back=1,
        benchmark="bench",
        fast_window=5,
        slow_window=10,
    )

    assert not seasonal.overlay_data.empty
    assert seasonal.overlay_data["benchmark_return"].notna().all()
    assert seasonal.overlay_data.groupby("year")["benchmark_return"].std().gt(0).all()


def test_run_and_save_same_window_previous_years_persists_overlay_files(tmp_path: Path):
    data = _build_price_data(seed=21)
    bt = VectorBacktester(data=data)

    result = bt.run_and_save_same_window_previous_years(
        output_dir=tmp_path,
        start_date="2025-01-02",
        end_date="2025-04-30",
        years_back=2,
        benchmark="bench",
        benchmark_label="SPY",
        fast_window=5,
        slow_window=10,
        quick=True,
    )

    metrics_path = Path(result["metrics_path"])
    overlay_path = Path(result["overlay_path"])

    assert metrics_path.is_file()
    assert overlay_path.is_file()
    assert "seasonal_result" in result
    assert not result["seasonal_result"].overlay_data.empty

    overlay_df = pd.read_csv(overlay_path)
    assert "benchmark_label" in overlay_df.columns
    assert (overlay_df["benchmark_label"] == "SPY").all()


def test_run_same_window_previous_years_uses_existing_strategy_returns_when_present():
    data = _build_price_data(seed=29)
    bt = VectorBacktester(data=data)

    strategy_returns = data["close"].pct_change().fillna(0.0) * 0.6
    market_returns = data["bench"].pct_change().fillna(0.0)
    bt.run_from_returns(
        strategy_returns=strategy_returns,
        market_returns=market_returns,
        close_series=data["close"],
    )

    seasonal = bt.run_same_window_previous_years(
        start_date="2025-01-02",
        end_date="2025-04-30",
        years_back=1,
        benchmark="bench",
        fast_window=5,
        slow_window=10,
    )

    assert not seasonal.overlay_data.empty

    for _, group in seasonal.overlay_data.groupby("year"):
        time_col = "date" if "date" in group.columns else "day"
        dates = pd.to_datetime(group[time_col])
        src_returns = bt.data.loc[dates, "strategy_returns"].to_numpy(dtype=float)
        if src_returns.size > 0:
            src_returns[0] = 0.0
        expected = np.cumprod(1.0 + src_returns) - 1.0
        np.testing.assert_allclose(
            group["strategy_return"].to_numpy(dtype=float),
            expected,
            atol=1e-12,
        )


def test_run_uses_shifted_position_to_prevent_lookahead():
    idx = pd.date_range("2024-01-01", periods=200, freq="D")
    prices = np.linspace(100.0, 140.0, len(idx), dtype=np.float64)
    data = pd.DataFrame({"close": prices}, index=idx)

    backtester = VectorBacktester(data, commission=0.0, slippage=0.0)
    backtester.run(fast_window=10, slow_window=20, z_entry=10.0, z_exit=10.0)

    position = backtester.data["position"]
    # Use diff-based change detection so the first row is not treated as a change
    # due to shift-induced NaN comparison behavior.
    pos_change = position.diff().fillna(0.0).ne(0.0)
    changed_idx = position.index[pos_change & position.notna()]

    if len(changed_idx) == 0:
        pytest.skip("No position transitions generated for this synthetic path.")

    first_change = changed_idx[0]
    loc = backtester.data.index.get_loc(first_change)
    assert loc > 0

    # Return at change timestamp should still reflect prior position (0.0 here).
    assert float(backtester.data["strategy_returns"].iloc[loc]) == pytest.approx(
        0.0, abs=1e-12
    )


def test_transaction_costs_reduce_returns_on_turnover():
    idx = pd.date_range("2024-01-01", periods=240, freq="D")
    base = np.linspace(100.0, 110.0, len(idx))
    oscillation = 3.5 * np.sin(np.linspace(0, 22 * np.pi, len(idx)))
    prices = (base + oscillation).astype(np.float64)
    data = pd.DataFrame({"close": prices}, index=idx)

    no_cost = VectorBacktester(data.copy(), commission=0.0, slippage=0.0)
    no_cost.run(fast_window=8, slow_window=21, z_entry=1.2, z_exit=0.25)

    with_cost = VectorBacktester(data.copy(), commission=0.0025, slippage=0.0015)
    with_cost.run(fast_window=8, slow_window=21, z_entry=1.2, z_exit=0.25)

    turnover = with_cost.data["position"].diff().abs().fillna(0.0).sum()
    if turnover == 0:
        pytest.skip("No turnover generated for this synthetic path/parameters.")

    assert with_cost.metrics["total_return"] < no_cost.metrics["total_return"]


def test_run_same_window_previous_years_handles_tz_aware_index_with_naive_bounds():
    data = _build_price_data(seed=11)
    data.index = data.index.tz_localize("America/New_York")
    bt = VectorBacktester(data=data)

    seasonal = bt.run_same_window_previous_years(
        start_date="2025-01-02",
        end_date="2025-04-30",
        years_back=2,
        fast_window=5,
        slow_window=10,
    )

    assert not seasonal.per_year_metrics.empty
    assert not seasonal.overlay_data.empty


def test_monte_carlo_bootstrap_returns_expected_shape_and_stats_keys():
    data = _build_price_data(seed=13)
    bt = VectorBacktester(data=data)
    bt.run(fast_window=5, slow_window=10, z_entry=1.5, z_exit=0.25, quick=True)

    paths, stats = bt.monte_carlo(n_sims=250, method="bootstrap", seed=123)

    expected_periods = int(bt.data["strategy_returns"].dropna().shape[0])
    assert paths.shape == (250, expected_periods)
    assert set(
        [
            "n_sims",
            "periods",
            "method",
            "mean_return",
            "median_return",
            "p10_return",
            "p90_return",
            "max_drawdown_p5",
        ]
    ).issubset(stats.keys())


def test_monte_carlo_is_deterministic_with_seed():
    data = _build_price_data(seed=19)
    bt = VectorBacktester(data=data)
    bt.run(fast_window=5, slow_window=10, z_entry=1.5, z_exit=0.25, quick=True)

    paths_1, stats_1 = bt.monte_carlo(n_sims=120, method="bootstrap", seed=99)
    paths_2, stats_2 = bt.monte_carlo(n_sims=120, method="bootstrap", seed=99)

    np.testing.assert_allclose(paths_1, paths_2)
    assert stats_1 == stats_2


def test_monte_carlo_parametric_and_block_bootstrap_supported():
    data = _build_price_data(seed=23)
    bt = VectorBacktester(data=data)
    bt.run(fast_window=5, slow_window=10, z_entry=1.5, z_exit=0.25, quick=True)

    param_paths, param_stats = bt.monte_carlo(n_sims=80, method="parametric", seed=42)
    block_paths, block_stats = bt.monte_carlo(
        n_sims=80,
        method="bootstrap",
        block_size=7,
        seed=42,
    )

    assert param_paths.shape[0] == 80
    assert block_paths.shape[0] == 80
    assert param_stats["method"] == "parametric"
    assert block_stats["method"] == "bootstrap"
    assert block_stats["block_size"] == 7


def test_monte_carlo_requires_strategy_returns():
    data = _build_price_data(seed=31)
    bt = VectorBacktester(data=data)

    with pytest.raises(ValueError, match="Strategy returns not found"):
        bt.monte_carlo(n_sims=50)


def test_run_from_returns_populates_metrics_and_curves():
    idx = pd.date_range("2025-01-01", periods=6, freq="D")
    close = pd.Series([100, 101, 102, 100, 103, 104], index=idx, dtype=float)
    strategy_returns = pd.Series([0.0, 0.01, -0.005, 0.02, 0.0, 0.01], index=idx)
    benchmark_returns = close.pct_change().fillna(0.0)

    bt = VectorBacktester(data=pd.DataFrame({"close": close}, index=idx))
    results = bt.run_from_returns(
        strategy_returns=strategy_returns,
        market_returns=benchmark_returns,
        close_series=close,
    )

    assert not results.empty
    assert set(["strategy_returns", "cum_strategy", "cum_market"]).issubset(
        results.columns
    )
    assert bt.metrics is not None
    assert abs(bt.metrics["total_return"]) > 1e-8
