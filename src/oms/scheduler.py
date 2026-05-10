from __future__ import annotations

import heapq
import logging
import threading
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from itertools import count
from typing import Any, Callable, Iterable, Optional


LOGGER = logging.getLogger(__name__)


@dataclass(order=True)
class _QueueItem:
    due_ts: float
    seq: int
    child_order: Any = field(compare=False)
    attempts: int = field(default=0, compare=False)


class AlgoScheduler:
    """Runs child orders on schedule in a dedicated thread."""

    def __init__(
        self,
        executor: Any,
        order_manager: Optional[Any] = None,
        *,
        tick_seconds: float = 5.0,
        max_retries: int = 1,
        retry_delay_seconds: Optional[float] = None,
        now_fn: Optional[Callable[[], datetime]] = None,
    ) -> None:
        self._executor = executor
        self._order_manager = order_manager
        self._tick_seconds = max(0.1, float(tick_seconds))
        self._max_retries = max(0, int(max_retries))
        self._retry_delay_seconds = (
            self._tick_seconds
            if retry_delay_seconds is None
            else max(0.0, float(retry_delay_seconds))
        )
        self._now_fn = now_fn or (lambda: datetime.now(timezone.utc))

        self._lock = threading.Lock()
        self._wake_event = threading.Event()
        self._running = False
        self._thread: Optional[threading.Thread] = None

        # Heap keeps the soonest order at index 0.
        self._heap: list[_QueueItem] = []
        self._seq = count()

    @property
    def running(self) -> bool:
        return self._running

    def next_due_time(self) -> Optional[datetime]:
        with self._lock:
            if not self._heap:
                return None
            return datetime.fromtimestamp(self._heap[0].due_ts, tz=timezone.utc)

    def start(self) -> None:
        with self._lock:
            if self._running:
                return
            self._running = True
            self._thread = threading.Thread(
                target=self._run_loop,
                name="oms-algo-scheduler",
                daemon=True,
            )
            self._thread.start()
        LOGGER.info("AlgoScheduler started.")

    def stop(
        self,
        *,
        wait: bool = True,
        timeout: Optional[float] = 10.0,
        cancel_pending: bool = True,
    ) -> None:
        with self._lock:
            self._running = False
            pending_items = list(self._heap) if cancel_pending else []
            if cancel_pending:
                self._heap.clear()
        self._wake_event.set()

        thread = self._thread
        if wait and thread and thread.is_alive():
            thread.join(timeout=timeout)

        for item in pending_items:
            self._notify_child_cancelled(item.child_order)

        LOGGER.info("AlgoScheduler stopped.")

    def enqueue(self, child_order: Any) -> None:
        self._enqueue_with_attempts(child_order=child_order, attempts=0)

    def enqueue_many(self, child_orders: Iterable[Any]) -> None:
        for child_order in child_orders:
            self.enqueue(child_order)

    def pending_count(self) -> int:
        with self._lock:
            return len(self._heap)

    def _enqueue_with_attempts(
        self,
        *,
        child_order: Any,
        attempts: int,
        due_ts: Optional[float] = None,
    ) -> None:
        if due_ts is None:
            due_ts = self._to_epoch_seconds(getattr(child_order, "scheduled_time", None))
        item = _QueueItem(
            due_ts=due_ts,
            seq=next(self._seq),
            child_order=child_order,
            attempts=attempts,
        )
        with self._lock:
            heapq.heappush(self._heap, item)
        self._wake_event.set()

    def _run_loop(self) -> None:
        # Keep the loop small: pop due items, execute, then wait.
        while True:
            if not self._running:
                return

            due_batch = self._pop_due_orders()
            if not due_batch:
                self._wake_event.wait(timeout=self._tick_seconds)
                self._wake_event.clear()
                continue

            for item in due_batch:
                self._execute_item(item)

    def _pop_due_orders(self) -> list[_QueueItem]:
        now_ts = self._to_epoch_seconds(self._now_fn())
        due: list[_QueueItem] = []

        with self._lock:
            while self._heap and self._heap[0].due_ts <= now_ts:
                due.append(heapq.heappop(self._heap))

        return due

    def _execute_item(self, item: _QueueItem) -> None:
        child_order = item.child_order
        child_id = getattr(child_order, "child_id", "<unknown-child>")

        try:
            fill_result = self._executor.execute_child_order(child_order)
            self._notify_child_filled(child_order, fill_result)
            return
        except Exception:
            LOGGER.exception("Child order execution failed for %s", child_id)

        if item.attempts < self._max_retries:
            LOGGER.warning(
                "Retrying child order %s (%d/%d)",
                child_id,
                item.attempts + 1,
                self._max_retries,
            )
            self._enqueue_with_attempts(
                child_order=child_order,
                attempts=item.attempts + 1,
                due_ts=(self._to_epoch_seconds(self._now_fn()) + self._retry_delay_seconds),
            )
            return

        # The scheduler reports terminal failure; lifecycle state belongs upstream.
        self._notify_child_cancelled(child_order)

    def _notify_child_filled(self, child_order: Any, fill_result: Any) -> None:
        if self._order_manager and hasattr(self._order_manager, "on_child_filled"):
            self._order_manager.on_child_filled(child_order, fill_result)

    def _notify_child_cancelled(self, child_order: Any) -> None:
        if not self._order_manager:
            return

        callback = getattr(self._order_manager, "on_child_cancelled", None)
        if not callable(callback):
            return

        try:
            callback(child_order, reason="scheduler-cancelled")
        except TypeError:
            callback(child_order)

    @staticmethod
    def _to_epoch_seconds(value: Any) -> float:
        if value is None:
            return time.time()

        if isinstance(value, datetime):
            dt = value
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            return dt.timestamp()

        if isinstance(value, (int, float)):
            return float(value)

        return time.time()

