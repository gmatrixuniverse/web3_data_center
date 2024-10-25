from typing import Dict, Any, Optional, List
from .base_client import BaseClient
from ..models.holder import Holder
from ..models.token import Token

class SolscanClient(BaseClient):
    def __init__(self, config_path: str = "config.yml", use_proxy: bool = False):
        super().__init__('solscan', config_path=config_path, use_proxy=use_proxy)

    async def get_token_info(self, token_address: str) -> Optional[Token]:
        endpoint = f"/token/meta"
        params = {"address": token_address}
        headers = {
            'accept': 'application/json',
            'user-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36'
        }
        response = await self._make_request(endpoint, params=params, headers=headers)
        
        if response and response.get('success'):
            data = response['data']
            return Token(
                address=data['address'],
                name=data['name'],
                symbol=data['symbol'],
                decimals=data['decimals'],
                total_supply=data['supply'],
                holder_count=data['holder'],
                chain='solana'  # Assuming all tokens from Solscan are on Solana
            )
        return None

    async def get_top_holders(self, token_address: str, page: int = 1, page_size: int = 10, 
                              from_amount: int = 0, to_amount: int = 10000000000) -> Optional[List[Holder]]:
        
        if page_size not in [10, 20, 30, 40]:
            raise ValueError("Invalid page_size. Must be one of 10, 20, 30, or 40.")

        endpoint = f"/token/holders"
        params = {
            "address": token_address,
            "page": page,
            "page_size": page_size,
            "from_amount": from_amount,
            "to_amount": to_amount
        }
        headers = {
            'accept': 'application/json',
            'user-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36'
        }
        response = await self._make_request(endpoint, params=params, headers=headers)
        
        if response and response.get('success'):
            data = response['data']
            return [Holder(
                address=item.get('owner', ''),
                token_address=token_address,
                amount=item['amount'],
                rank=item['rank'],
            ) for item in data['items']]
        return None