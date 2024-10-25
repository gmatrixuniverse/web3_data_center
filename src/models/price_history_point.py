from dataclasses import dataclass
from datetime import datetime
from typing import Optional

@dataclass
class PriceHistoryPoint:
    timestamp: datetime
    value: Optional[float] = None
    open: Optional[float] = None
    high: Optional[float] = None
    low: Optional[float] = None
    close: Optional[float] = None
    volume: Optional[float] = None
    market_cap: Optional[float] = None
    address: Optional[str] = None

    @classmethod
    def from_dict(cls, data: dict):
        timestamp = datetime.fromtimestamp(data['unixTime'] / 1000 if 'unixTime' in data else data['time'] / 1000)
        return cls(
            timestamp=timestamp,
            value=float(data['value']) if 'value' in data else None,
            open=float(data['open']) if 'open' in data else None,
            high=float(data['high']) if 'high' in data else None,
            low=float(data['low']) if 'low' in data else None,
            close=float(data['close']) if 'close' in data else None,
            volume=float(data['volume']) if 'volume' in data else None,
            market_cap=float(data['marketCap']) if 'marketCap' in data else None,
            address=data['address'] if 'address' in data else None
        )

    def to_dict(self):
        return {
            'unixTime': int(self.timestamp.timestamp() * 1000),
            'value': self.value,
            'open': self.open,
            'high': self.high,
            'low': self.low,
            'close': self.close,
            'volume': self.volume,
            'marketCap': self.market_cap,
            'address': self.address
        }
