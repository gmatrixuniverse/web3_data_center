from dataclasses import dataclass, asdict
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
            name=data.get('name', ''),
            symbol=data['symbol'],
            decimals=data.get('decimals', None),
            total_supply=data.get('total_supply', None),
            market_cap=float(data['market_cap']) if data.get('market_cap') is not None else None,
            price=float(data['price']) if data.get('price') is not None else None,
            volume_24h=float(data['volume_24h']) if data.get('volume_24h') is not None else None,
            liquidity=float(data['liquidity']) if data.get('liquidity') is not None else None,
            holder_count=int(data['holder_count']) if data.get('holder_count') is not None else None,
            swap_count_24h=int(data['swaps_24h']) if data.get('swaps_24h') is not None else None,
            created_at=datetime.fromtimestamp(data['open_timestamp']) if data.get('open_timestamp') is not None else None,
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

    # serialize to dict
    def to_dict(self):
        data = asdict(self)
        # Convert datetime to ISO format string
        if isinstance(self.created_at, datetime):
            data['created_at'] = self.created_at.isoformat()
        return data

class RankedToken(NamedTuple):
    token: Token
    rank: int

def create_ranked_tokens_from_gmgn(data: List[dict], limit: int = 100) -> List[RankedToken]:
    tokens = [Token.from_gmgn(item) for item in data[:limit]]
    return [RankedToken(token, index + 1) for index, token in enumerate(tokens)]