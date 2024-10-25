from dataclasses import dataclass
from typing import Optional
from datetime import datetime

@dataclass
class Transaction:
    tx_hash: str
    block_number: int
    timestamp: datetime
    from_address: str
    to_address: str
    amount: float
    token_address: str
    fee: Optional[float] = None
    status: Optional[str] = None

    def __post_init__(self):
        if isinstance(self.timestamp, str):
            self.timestamp = datetime.fromisoformat(self.timestamp.rstrip('Z'))

    @classmethod
    def from_gmgn(cls, data: dict) -> 'Transaction':
        return cls(
            tx_hash=data['tx_hash'],
            block_number=int(data['block_number']),
            timestamp=data['timestamp'],
            from_address=data['from'],
            to_address=data['to'],
            amount=float(data['amount']),
            token_address=data['token_address'],
            fee=float(data['fee']) if 'fee' in data else None,
            status=data.get('status')
        )

    @classmethod
    def from_birdeye(cls, data: dict) -> 'Transaction':
        # Implement conversion from Birdeye API data to Transaction
        pass

    @classmethod
    def from_solscan(cls, data: dict) -> 'Transaction':
        # Implement conversion from Solscan API data to Transaction
        pass