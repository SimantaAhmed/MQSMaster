from __future__ import annotations

import threading
import time
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone

from src.oms.scheduler import AlgoScheduler


@dataclass
class _ChildOrder:
    child_id: str
    scheduled_time: datetime


class _Executor:
    def __init__(self, *, fail_first_n: int = 0) -> None:
        self.fail_first_n = fail_first_n
        self.call_times: list[float] = []

    def execute_child_order(self, child_order):
        self.call_times.append(time.monotonic())
        if len(self.call_times) <= self.fail_first_n:
            raise RuntimeError(f"synthetic failure for {child_order.child_id}")
        return {"status": "success", "quantity": 1}


class _OrderManager:
    def __init__(self) -> None:
        self.filled: list[str] = []
        self.cancelled: list[str] = []
        self.fill_event = threading.Event()
        self.cancel_event = threading.Event()

    def on_child_filled(self, child_order, fill_result) -> None:
        self.filled.append(child_order.child_id)
        self.fill_event.set()

    def on_child_cancelled(self, child_order, reason=None) -> None:
        self.cancelled.append(child_order.child_id)
        self.cancel_event.set()


def test_scheduler_executes_due_child_order():
    executor = _Executor()
    manager = _OrderManager()
    scheduler = AlgoScheduler(executor=executor, order_manager=manager, tick_seconds=0.01)

    child = _ChildOrder(
        child_id="due-child",
        scheduled_time=datetime.now(timezone.utc),
    )

    scheduler.start()
    try:
        scheduler.enqueue(child)
        assert manager.fill_event.wait(0.5)
    finally:
        scheduler.stop()

    assert manager.filled == ["due-child"]
    assert manager.cancelled == []
    assert len(executor.call_times) == 1


def test_scheduler_retries_on_next_tick_then_cancels():
    executor = _Executor(fail_first_n=2)
    manager = _OrderManager()
    scheduler = AlgoScheduler(
        executor=executor,
        order_manager=manager,
        tick_seconds=0.02,
        max_retries=1,
    )

    child = _ChildOrder(
        child_id="retry-child",
        scheduled_time=datetime.now(timezone.utc),
    )

    scheduler.start()
    try:
        scheduler.enqueue(child)
        assert manager.cancel_event.wait(0.75)
    finally:
        scheduler.stop()

    assert manager.filled == []
    assert manager.cancelled == ["retry-child"]
    assert len(executor.call_times) == 2
    assert executor.call_times[1] - executor.call_times[0] >= 0.015


def test_scheduler_stop_cancels_pending_orders():
    executor = _Executor()
    manager = _OrderManager()
    scheduler = AlgoScheduler(executor=executor, order_manager=manager, tick_seconds=0.01)

    child = _ChildOrder(
        child_id="pending-child",
        scheduled_time=datetime.now(timezone.utc) + timedelta(seconds=60),
    )

    scheduler.start()
    scheduler.enqueue(child)
    scheduler.stop(cancel_pending=True)

    assert manager.cancelled == ["pending-child"]
    assert manager.filled == []
    assert executor.call_times == []