from __future__ import annotations

import logging
import math
from datetime import datetime, timezone
from typing import Any, Optional

import pandas as pd

try:
	from oms.order_structs import ChildOrder, OrderStatus, ParentOrder, Side
except ImportError:
	from src.oms.order_structs import ChildOrder, OrderStatus, ParentOrder, Side


LOGGER = logging.getLogger(__name__)


class OrderManager:
	"""Coordinates order submission and receives fill callbacks from scheduler."""

	def __init__(self, executor: Any, scheduler: Optional[Any] = None) -> None:
		self._executor = executor
		self._scheduler = scheduler
		self._parents: dict[str, ParentOrder] = {}

	def bind_scheduler(self, scheduler: Any) -> None:
		self._scheduler = scheduler

	def submit_order(
		self,
		*,
		portfolio_id: str,
		ticker: str,
		side: str,
		confidence: float,
		arrival_price: float,
		cash: float,
		positions: pd.DataFrame,
		port_notional: float,
		ticker_weight: float,
		timestamp: Optional[datetime] = None,
	) -> Optional[str]:
		if self._scheduler is None:
			LOGGER.warning("OrderManager received order with no scheduler bound.")
			return None

		if arrival_price is None or float(arrival_price) <= 0:
			LOGGER.warning("Skipping order for %s due to invalid arrival_price.", ticker)
			return None

		side_value = side.upper().strip()
		if side_value not in (Side.BUY.value, Side.SELL.value):
			LOGGER.warning("Skipping order for %s due to unsupported side=%s.", ticker, side)
			return None

		confidence_val = max(0.0, min(1.0, float(confidence)))
		if confidence_val <= 0:
			return None

		now = timestamp or datetime.now(timezone.utc)
		current_row = positions[positions["ticker"] == ticker]
		current_qty = (
			float(current_row["quantity"].iloc[0]) if not current_row.empty else 0.0
		)

		target_notional = float(port_notional) * float(ticker_weight)
		if side_value == Side.SELL.value:
			target_notional *= -1

		adjustment_notional = target_notional - (current_qty * float(arrival_price))
		desired_trade_notional = adjustment_notional * confidence_val
		if abs(desired_trade_notional) < 1.0:
			return None

		buying_power = self._safe_buying_power(
			portfolio_equity=float(port_notional),
			positions_df=positions,
			ticker=ticker,
			current_price=float(arrival_price),
		)

		if desired_trade_notional > 0:
			final_trade_notional = min(abs(desired_trade_notional), float(cash), buying_power)
		else:
			final_trade_notional = min(abs(desired_trade_notional), buying_power)

		if final_trade_notional < 1.0:
			return None

		quantity_to_trade = math.floor(final_trade_notional / float(arrival_price))
		if quantity_to_trade <= 0:
			return None

		parent = ParentOrder(
			portfolio_id=str(portfolio_id),
			ticker=ticker,
			side=Side(side_value),
			total_quantity=float(quantity_to_trade),
			status=OrderStatus.WORKING,
			created_at=now,
			updated_at=now,
		)
		self._parents[parent.order_id] = parent

		child = ChildOrder(
			parent_order_id=parent.order_id,
			portfolio_id=str(portfolio_id),
			ticker=ticker,
			side=Side(side_value),
			target_quantity=float(quantity_to_trade),
			scheduled_time=now,
			status=OrderStatus.PENDING,
			slice_index=0,
			cash_before=float(cash),
			current_quantity_before=float(current_qty),
			port_notional_before=float(port_notional),
			arrival_price=float(arrival_price),
		)
		self._scheduler.enqueue(child)
		return parent.order_id

	def on_child_filled(self, child_order: ChildOrder, fill_result: Any) -> None:
		parent = self._parents.get(child_order.parent_order_id)
		if not parent:
			return

		filled_qty = float(getattr(child_order, "filled_quantity", 0.0) or 0.0)
		if not filled_qty and isinstance(fill_result, dict):
			filled_qty = float(fill_result.get("quantity", 0.0) or 0.0)

		parent.filled_quantity += filled_qty
		parent.updated_at = datetime.now(timezone.utc)
		if parent.filled_quantity >= parent.total_quantity:
			parent.status = OrderStatus.FILLED
		elif parent.filled_quantity > 0:
			parent.status = OrderStatus.PARTIALLY_FILLED

	def on_child_cancelled(self, child_order: ChildOrder) -> None:
		parent = self._parents.get(child_order.parent_order_id)
		if not parent:
			return
		parent.status = OrderStatus.CANCELLED
		parent.updated_at = datetime.now(timezone.utc)

	def _safe_buying_power(
		self,
		*,
		portfolio_equity: float,
		positions_df: pd.DataFrame,
		ticker: str,
		current_price: float,
	) -> float:
		calc = getattr(self._executor, "_calculate_buying_power", None)
		if callable(calc):
			try:
				return float(calc(portfolio_equity, positions_df, ticker, current_price))
			except Exception:
				LOGGER.exception("Falling back after buying power calculation error.")

		return max(0.0, float(portfolio_equity))

