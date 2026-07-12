"""
kronos_trade/execution/brokers/base.py
Abstract base for all broker adapters.
"""
from __future__ import annotations

from abc import ABC, abstractmethod

from kronos_trade.models import (
    AccountSnapshot, BracketOrder, BrokerName, Position,
)


class BrokerAdapter(ABC):
    """
    Each broker connector inherits this class.
    All methods are async.
    """

    @property
    @abstractmethod
    def name(self) -> BrokerName: ...

    @property
    def supported_symbols(self) -> set[str] | None:
        """
        Live set of tradeable symbols, or None if broker accepts all.
        Populated durring connect(). Ovveride in adapters that can fetch this.
        """
        return None

    @abstractmethod
    async def connect(self) -> None:
        """Authenticate and establish connection."""
        ...

    @abstractmethod
    async def disconnect(self) -> None: ...

    @abstractmethod
    async def submit_bracket_order(self, order: BracketOrder) -> BracketOrder:
        """
        Submit a bracket order (entry + stop + TP).
        Returns the order with broker_order_id and status updated.
        """
        ...

    @abstractmethod
    async def cancel_order(self, broker_order_id: str) -> bool: ...

    @abstractmethod
    async def get_positions(self) -> list[Position]: ...

    @abstractmethod
    async def get_account(self) -> AccountSnapshot: ...

    @abstractmethod
    async def close_position(self, symbol: str) -> bool:
        """Market-close an open position for `symbol`."""
        ...
