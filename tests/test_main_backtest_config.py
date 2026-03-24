import src.main_backtest as main_backtest


class _DummyBacktestEngine:
    def __init__(self, db_connector, backtest_executor=None):
        self.db_connector = db_connector
        self.backtest_executor = backtest_executor
        self.setup_kwargs = None
        self.ran = False

    def setup(self, **kwargs):
        self.setup_kwargs = kwargs

    def run(self):
        self.ran = True


def test_main_uses_passed_portfolios_and_merged_fast_config(monkeypatch):
    created = []

    def _engine_factory(db_connector, backtest_executor=None):
        engine = _DummyBacktestEngine(db_connector, backtest_executor)
        created.append(engine)
        return engine

    monkeypatch.setattr(main_backtest, "MQSDBConnector", lambda: object())
    monkeypatch.setattr(main_backtest, "BacktestEngine", _engine_factory)

    custom_portfolios = [object]
    engine = main_backtest.main(
        portfolio_classes=custom_portfolios,
        backtest_mode="fast",
        fast_years_back=4,
        fast_benchmark_label="QQQ",
        fast_config={"mc_enabled": True, "mc_n_sims": 321},
    )

    assert engine is created[0]
    assert engine.ran is True
    assert engine.setup_kwargs is not None
    assert engine.setup_kwargs["portfolio_classes"] == custom_portfolios

    fast_cfg = engine.setup_kwargs["fast_config"]
    assert fast_cfg["years_back"] == 4
    assert fast_cfg["benchmark_label"] == "QQQ"
    assert fast_cfg["mc_enabled"] is True
    assert fast_cfg["mc_n_sims"] == 321
    assert "mc_method" in fast_cfg


def test_main_uses_default_portfolios_when_none(monkeypatch):
    created = []

    def _engine_factory(db_connector, backtest_executor=None):
        engine = _DummyBacktestEngine(db_connector, backtest_executor)
        created.append(engine)
        return engine

    monkeypatch.setattr(main_backtest, "MQSDBConnector", lambda: object())
    monkeypatch.setattr(main_backtest, "BacktestEngine", _engine_factory)

    main_backtest.main()

    assert len(created) == 1
    assert created[0].ran is True
    assert created[0].setup_kwargs["portfolio_classes"] == list(
        main_backtest.DEFAULT_PORTFOLIO_CLASSES
    )


def test_main_preserves_fast_mode_defaults_without_legacy_overrides(monkeypatch):
    created = []

    def _engine_factory(db_connector, backtest_executor=None):
        engine = _DummyBacktestEngine(db_connector, backtest_executor)
        created.append(engine)
        return engine

    monkeypatch.setattr(main_backtest, "MQSDBConnector", lambda: object())
    monkeypatch.setattr(main_backtest, "BacktestEngine", _engine_factory)

    main_backtest.main(backtest_mode="fast")

    assert len(created) == 1
    assert created[0].ran is True
    fast_cfg = created[0].setup_kwargs["fast_config"]
    assert fast_cfg["years_back"] == main_backtest.FAST_MODE_CONFIG["years_back"]
    assert (
        fast_cfg["benchmark_label"] == main_backtest.FAST_MODE_CONFIG["benchmark_label"]
    )
