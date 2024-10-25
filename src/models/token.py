from dataclasses import dataclass
from typing import Optional, List, NamedTuple
from datetime import datetime

@dataclass
class Token:
    address: str
    name: str
    symbol: str
    decimals: Optional[int] = None
    total_supply: Optional[float] = None
    price: Optional[float] = None
    holder_count: Optional[int] = None
    market_cap: Optional[float] = None
    liquidity: Optional[float] = None
    volume_24h: Optional[float] = None
    swap_count_24h: Optional[int] = None
    created_at: Optional[datetime] = None
    chain: str = 'solana'

    def __post_init__(self):
        if isinstance(self.created_at, str):
            self.created_at = datetime.fromisoformat(self.created_at.rstrip('Z'))

    @classmethod
    def from_gmgn(cls, data: dict) -> 'Token':
        return cls(
            address=data['address'],
            name=data.get('name', ''),  # Name is not provided in the sample, use empty string as default
            symbol=data['symbol'],
            decimals=None,  # Decimals are not provided in the sample
            total_supply=None,  # Total supply is not provided in the sample
            market_cap=float(data['market_cap']) if 'market_cap' in data else None,
            price=float(data['price']) if 'price' in data else None,
            volume_24h=float(data['volume']) if 'volume' in data else None,
            liquidity=float(data['liquidity']) if 'liquidity' in data else None,
            holder_count=int(data['holder_count']) if 'holder_count' in data else None,
            swap_count_24h=int(data['swaps_24h']) if 'swaps_24h' in data else None,
            created_at=datetime.fromtimestamp(data['open_timestamp']) if 'open_timestamp' in data else None,
            chain=data.get('chain', 'solana')
        )

    @classmethod
    def from_birdeye(cls, data: dict) -> 'Token':
        # Implement conversion from Birdeye API data to Token
        pass

    @classmethod
    def from_solscan(cls, data: dict) -> 'Token':
        # Implement conversion from Solscan API data to Token
        pass

class RankedToken(NamedTuple):
    token: Token
    rank: int

def create_ranked_tokens_from_gmgn(data: List[dict], limit: int = 100) -> List[RankedToken]:
    tokens = [Token.from_gmgn(item) for item in data[:limit]]
    return [RankedToken(token, index + 1) for index, token in enumerate(tokens)]