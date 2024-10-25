from dataclasses import dataclass, asdict
from typing import Optional, List

@dataclass
class Holder:
    address: str
    token_address: str
    amount: float
    rank: Optional[int] = None
    percentage: Optional[float] = None
    last_active_timestamp: Optional[int] = None
    total_trades: Optional[int] = None
    buy_count: Optional[int] = None
    sell_count: Optional[int] = None
    buy_volume: Optional[float] = None
    sell_volume: Optional[float] = None
    profitable_trades: Optional[int] = None
    total_profit: Optional[float] = None
    avg_holding_time: Optional[float] = None
    pnl: Optional[float] = None
    tags: Optional[List[str]] = None

    @classmethod
    def from_gmgn(cls, data: dict, token_address: str):
        return cls(
            address=data['address'],
            token_address=token_address,
            amount=float(data['amount']),
            percentage=float(data['percentage']),
            last_active_timestamp=data.get('last_active_timestamp'),
            total_trades=data.get('total_trades'),
            profitable_trades=data.get('profitable_trades'),
            total_profit=data.get('total_profit'),
            avg_holding_time=data.get('avg_holding_time'),
            pnl=data.get('pnl')
        )
    
    @classmethod
    def from_birdeye(cls, data: dict, token_address: str) -> 'Holder':
        # Implement conversion from Birdeye API data to Holder
        pass

    @classmethod
    def from_solscan(cls, data: dict, token_address: str) -> 'Holder':
        # Implement conversion from Solscan API data to Holder
        pass

    def to_dict(self) -> dict:
        return asdict(self)
