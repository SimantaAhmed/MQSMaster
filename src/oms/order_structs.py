from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from typing import Optional
import uuid


class OrderStatus(Enum):
	PENDING = "PENDING"
	WORKING = "WORKING"
	PARTIALLY_FILLED = "PARTIALLY_FILLED"
	FILLED = "FILLED"
	CANCELLED = "CANCELLED"


class Side(Enum):
	BUY = "BUY"
	SELL = "SELL"


@dataclass
class ParentOrder:
	order_id: str = field(default_factory=lambda: str(uuid.uuid4()))
	portfolio_id: str = ""
	ticker: str = ""
	side: Side = Side.BUY
	total_quantity: float = 0.0
	filled_quantity: float = 0.0
	status: OrderStatus = OrderStatus.PENDING
	created_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
	updated_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))


@dataclass
class ChildOrder:
	child_id: str = field(default_factory=lambda: str(uuid.uuid4()))
	parent_order_id: str = ""
	portfolio_id: str = ""
	ticker: str = ""
	side: Side = Side.BUY
	target_quantity: float = 0.0
	filled_quantity: float = 0.0
	scheduled_time: Optional[datetime] = None
	status: OrderStatus = OrderStatus.PENDING
	slice_index: int = 0

	# State snapshot used by executor.update_database
	cash_before: float = 0.0
	current_quantity_before: float = 0.0
	port_notional_before: float = 0.0
	arrival_price: float = 0.0

