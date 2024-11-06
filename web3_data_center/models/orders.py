from enum import Enum, auto
from dataclasses import dataclass
from typing import Any, Optional, Dict
from decimal import Decimal
from datetime import datetime

class OrderType(Enum):
    MARKET = auto()
    LIMIT = auto()
    STOP_LOSS = auto()
    STOP_LIMIT = auto()
    TRAILING_STOP = auto()

class OrderSide(Enum):
    BUY = auto()
    SELL = auto()

class OrderStatus(Enum):
    OPEN = auto()
    CLOSED = auto()
    CANCELED = auto()
    EXPIRED = auto()
    REJECTED = auto()
    PENDING = auto()
    PARTIALLY_FILLED = auto()

class OrderSource(Enum):
    CEX = auto()  # Centralized Exchange
    DEX = auto()  # Decentralized Exchange

@dataclass
class Order:
    id: str
    symbol: str
    type: OrderType
    side: OrderSide
    amount: Decimal
    price: Optional[Decimal] = None
    status: OrderStatus = OrderStatus.PENDING
    filled: Decimal = Decimal('0')
    remaining: Optional[Decimal] = None
    cost: Optional[Decimal] = None
    fee: Optional[Dict[str, Any]] = None
    timestamp: datetime = None
    last_update: datetime = None
    source: OrderSource = None
    exchange_id: str = None
    tx_hash: Optional[str] = None
    raw_data: Optional[Dict[str, Any]] = None

    def __post_init__(self):
        if isinstance(self.amount, (int, float)):
            self.amount = Decimal(str(self.amount))
        if isinstance(self.price, (int, float)):
            self.price = Decimal(str(self.price))
        if isinstance(self.filled, (int, float)):
            self.filled = Decimal(str(self.filled))
        if self.timestamp is None:
            self.timestamp = datetime.utcnow()
        if self.last_update is None:
            self.last_update = self.timestamp
        if self.remaining is None:
            self.remaining = self.amount - self.filled

    @classmethod
    def from_ccxt(cls, ccxt_order: Dict[str, Any]) -> 'Order':
        return cls(
            id=str(ccxt_order['id']),
            symbol=ccxt_order['symbol'],
            type=OrderType[ccxt_order['type'].upper()],
            side=OrderSide[ccxt_order['side'].upper()],
            amount=Decimal(str(ccxt_order['amount'])),
            price=Decimal(str(ccxt_order['price'])) if ccxt_order.get('price') else None,
            status=OrderStatus[ccxt_order['status'].upper()],
            filled=Decimal(str(ccxt_order.get('filled', 0))),
            cost=Decimal(str(ccxt_order['cost'])) if ccxt_order.get('cost') else None,
            fee=ccxt_order.get('fee'),
            timestamp=datetime.fromtimestamp(ccxt_order['timestamp'] / 1000),
            last_update=datetime.fromtimestamp(ccxt_order['lastTradeTimestamp'] / 1000) if ccxt_order.get('lastTradeTimestamp') else None,
            source=OrderSource.CEX,
            exchange_id=ccxt_order.get('exchange', ''),
            raw_data=ccxt_order
        )

    @classmethod
    def from_web3(cls, web3_order: Dict[str, Any]) -> 'Order':
        return cls(
            id=web3_order['transactionHash'],
            symbol=web3_order.get('symbol', ''),
            type=OrderType.MARKET,  # Most DEX trades are market orders
            side=OrderSide.BUY if web3_order.get('is_buy', True) else OrderSide.SELL,
            amount=Decimal(str(web3_order.get('amount', 0))),
            price=Decimal(str(web3_order['price'])) if web3_order.get('price') else None,
            status=OrderStatus.CLOSED if web3_order.get('status') == 'success' else OrderStatus.REJECTED,
            filled=Decimal(str(web3_order.get('filled', 0))),
            timestamp=datetime.fromtimestamp(web3_order.get('timestamp', datetime.utcnow().timestamp())),
            source=OrderSource.DEX,
            tx_hash=web3_order['transactionHash'],
            raw_data=web3_order
        )

    def to_dict(self) -> Dict[str, Any]:
        return {
            'id': self.id,
            'symbol': self.symbol,
            'type': self.type.name,
            'side': self.side.name,
            'amount': str(self.amount),
            'price': str(self.price) if self.price else None,
            'status': self.status.name,
            'filled': str(self.filled),
            'remaining': str(self.remaining) if self.remaining else None,
            'cost': str(self.cost) if self.cost else None,
            'fee': self.fee,
            'timestamp': self.timestamp.isoformat() if self.timestamp else None,
            'last_update': self.last_update.isoformat() if self.last_update else None,
            'source': self.source.name if self.source else None,
            'exchange_id': self.exchange_id,
            'tx_hash': self.tx_hash,
        }