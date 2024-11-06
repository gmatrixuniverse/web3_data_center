from dataclasses import dataclass
from typing import List, Optional, Dict
from datetime import datetime

@dataclass
class Token:
    address: str
    name: Optional[str] = None
    symbol: Optional[str] = None

@dataclass
class TransactionCount:
    buys: int = 0
    sells: int = 0

@dataclass
class Transactions:
    m5: TransactionCount = TransactionCount()
    h1: TransactionCount = TransactionCount()
    h6: TransactionCount = TransactionCount()
    h24: TransactionCount = TransactionCount()

@dataclass
class Volume:
    h24: float = 0.0
    h6: float = 0.0
    h1: float = 0.0
    m5: float = 0.0

@dataclass
class PriceChange:
    m5: float = 0.0
    h1: float = 0.0
    h6: float = 0.0
    h24: float = 0.0

@dataclass
class Liquidity:
    usd: float = 0.0
    base: float = 0.0
    quote: float = 0.0

@dataclass
class Website:
    label: str
    url: str

@dataclass
class Social:
    type: str
    url: str

@dataclass
class Info:
    imageUrl: Optional[str] = None
    websites: List[Website] = None
    socials: List[Social] = None

    def __post_init__(self):
        if self.websites is None:
            self.websites = []
        if self.socials is None:
            self.socials = []

@dataclass
class Pair:
    chainId: str
    pairAddress: str
    dexId: Optional[str] = None
    url: Optional[str] = None
    labels: List[str] = None
    baseToken: Optional[Token] = None
    quoteToken: Optional[Token] = None
    priceNative: Optional[str] = None
    priceUsd: Optional[str] = None
    txns: Transactions = None
    volume: Volume = None
    priceChange: PriceChange = None
    liquidity: Liquidity = None
    fdv: Optional[float] = None
    marketCap: Optional[float] = None
    pairCreatedAt: Optional[datetime] = None
    info: Info = None

    def __post_init__(self):
        if self.labels is None:
            self.labels = []
        if self.txns is None:
            self.txns = Transactions()
        if self.volume is None:
            self.volume = Volume()
        if self.priceChange is None:
            self.priceChange = PriceChange()
        if self.liquidity is None:
            self.liquidity = Liquidity()
        if self.info is None:
            self.info = Info()
        
        # Convert timestamp from milliseconds to datetime if present
        if isinstance(self.pairCreatedAt, int):
            self.pairCreatedAt = datetime.fromtimestamp(self.pairCreatedAt / 1000)