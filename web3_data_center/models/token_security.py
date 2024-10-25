from dataclasses import dataclass
from typing import Optional, List

@dataclass
class TokenSecurity:
    creator_address: Optional[str] = None
    owner_address: Optional[str] = None
    creation_tx: Optional[str] = None
    creation_time: Optional[int] = None
    creation_slot: Optional[int] = None
    mint_tx: Optional[str] = None
    mint_time: Optional[int] = None
    mint_slot: Optional[int] = None
    creator_balance: Optional[float] = None
    owner_balance: Optional[float] = None
    owner_percentage: Optional[float] = None
    creator_percentage: Optional[float] = None
    metaplex_update_authority: Optional[str] = None
    metaplex_update_authority_balance: Optional[float] = None
    metaplex_update_authority_percent: Optional[float] = None
    mutable_metadata: Optional[bool] = None
    top10_holder_balance: Optional[float] = None
    top10_holder_percent: Optional[float] = None
    top10_user_balance: Optional[float] = None
    top10_user_percent: Optional[float] = None
    is_true_token: Optional[bool] = None
    total_supply: Optional[float] = None
    pre_market_holder: List[str] = None
    lock_info: Optional[dict] = None
    freezeable: Optional[bool] = None
    freeze_authority: Optional[str] = None
    transfer_fee_enable: Optional[bool] = None
    transfer_fee_data: Optional[dict] = None
    is_token_2022: Optional[bool] = None
    non_transferable: Optional[bool] = None

    @classmethod
    def from_dict(cls, data: dict):
        return cls(**{k: v for k, v in data.items() if k in cls.__annotations__})

    def to_dict(self):
        return {k: v for k, v in self.__dict__.items() if v is not None}
