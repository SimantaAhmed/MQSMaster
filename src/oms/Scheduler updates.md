Added the first OMS execution flow for live trading. This introduces a background scheduler for child orders, wires it into the live engine, and routes strategy trades through OMS when enabled.

## Changes

### Added OMS scheduler

- Background thread lifecycle.
- Priority queue by scheduled time.
- Due-order execution on each tick.
- One retry before cancellation.
- Fill and cancel callbacks to order manager.

### Added OMS order models

- ParentOrder and ChildOrder.
- Side and OrderStatus enums.

### Added OrderManager

- Accepts order submissions.
- Builds parent/child orders.
- Enqueues child orders into scheduler.
- Tracks fill and cancellation state.

### Added child-order execution in live executor

### Wired OMS into RunEngine

- Creates scheduler and order manager.
- Starts scheduler on run.
- Stops scheduler on shutdown.

### Updated strategy flow

- StrategyContext can use OrderManager.
- Trades go through OMS when enabled.
- Existing direct execution remains as fallback.

## Files Changed

- scheduler.py
- order_structs.py
- order_manager.py
- executor.py
- engine.py
- strategy.py
- strategy_api.py
