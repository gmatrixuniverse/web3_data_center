from typing import Dict, Any, Optional, List
from datetime import datetime
import csv
import json
from functools import lru_cache
import asyncio
import logging
from chain_index import get_chain_info

from .base_client import BaseClient
from ..models.token import Token
from ..models.holder import Holder

class GMGNAPIClient(BaseClient):
    def __init__(self, config_path: str = "config.yml", use_proxy: bool = False, use_zenrows: bool = True):
        super().__init__('gmgn', config_path=config_path, use_proxy=use_proxy, use_zenrows=use_zenrows)

        self.headers.update({
            'accept': 'application/json',
            'user-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36'
        })

    async def get_token_list(self, chain: str = 'sol', limit: int = 100, orderby: str = "marketcap", direction: str = 'desc', min_liquidity: int = 1000, max_liquidity: int = 30000000, min_marketcap: int = 1000, max_marketcap: int = 30000000, min_volume: int = 1000, max_volume: int = 5000000000, min_swaps: int = 1, max_swaps: int = 500000, min_holder_count: int = 1000, min_insider_rate: float = 0, max_insider_rate: float = 0.99, min_created: str = '1h', max_created: str = '10000h', renounced: bool = True, frozen: bool = True, distribed: bool = True, burn: bool = True, token_burnt: bool = False, creator_close: bool = False, creator_hold: bool = False) -> Optional[List[Token]]:
        chain = 'sol' if chain == 'solana' else chain
        endpoint = f"/rank/{chain}/swaps/24h"
        params = {
            "min_liquidity": min_liquidity,
            "max_liquidity": max_liquidity,
            "min_marketcap": min_marketcap,
            "max_marketcap": max_marketcap,
            "min_volume": min_volume,
            "max_volume": max_volume,
            "min_swaps": min_swaps,
            "max_swaps": max_swaps,
            "min_holder_count": min_holder_count,
            "min_insider_rate": min_insider_rate,
            "max_insider_rate": max_insider_rate,
            "min_created": min_created,
            "max_created": max_created
        }
        
        filters = [f for f, enabled in {
            "renounced": renounced,
            "frozen": frozen,
            "distribed": distribed,
            "burn": burn,
            "token burnt": token_burnt,
            "creator close": creator_close,
            "creator hold": creator_hold
        }.items() if enabled]
        
        if filters:
            params["filters[]"] = filters
        
        valid_orderby = [
            "price_change_percent1m", "price_change_percent5m", "price_change_percent1h",
            "volume", "price", "swaps", "holder_count", "marketcap", "liquidity", "poolcreation"
        ]
        if orderby in valid_orderby:
            params["orderby"] = orderby
        if direction in ['desc', 'asc']:
            params["direction"] = direction
        
        logging.debug(f"Calling _make_request with endpoint: {endpoint} and params: {params}")
        response = await self._make_request(endpoint, params=params)
        logging.debug(f"Response received: {response}")
        
        if response is None:
            logging.warning("Received None response from _make_request")
            return None
        
        if isinstance(response, dict) and 'data' in response and 'rank' in response['data']:
            tokens = response['data']['rank'][:limit]
            return [Token.from_gmgn(token) for token in tokens]
        else:
            logging.warning("Invalid response format")
            logging.debug(f"Full response: {response}")
            return None
    
    @lru_cache(maxsize=128)
    async def get_token_price_history(self, token_address: str, chain: str = 'eth', resolution: str = '1m', from_time: int = None, to_time: int = None) -> Optional[List[Dict[str, Any]]]:
        endpoint = f"/tokens/kline/{chain}/{token_address}"
        params = {
            "resolution": resolution,
            "from": from_time,
            "to": to_time
        }
        logging.info(f"Getting token price history for {token_address} with resolution {resolution} from {from_time} to {to_time}")
        return await self._make_request(endpoint, params=params)

    def filter_hot_tokens(self, tokens: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        return [
            token for token in tokens
            if all([
                float(token['liquidity']) / float(token['market_cap']) > 0.05,
                float(token['volume']) > 0.25 * float(token['market_cap']),
                int(token['holder_count']) > 1000,
                int(token['swaps']) > 2000
            ])
        ]

    async def get_top_holders(self, token_address: str, limit: int = 20, chain: str = 'sol') -> Optional[List[Dict[str, Any]]]:
        endpoint = f"/tokens/top_holders/{chain}/{token_address}"
        params = {
            "limit": limit,
            "cost": 20,
            "tag": "All",
            "orderby": "amount_percentage",
            "direction": "desc"
        }
        response = await self._make_request(endpoint, params=params)
        if isinstance(response, list):
            return response[:limit]
        return None

    async def get_new_pairs(self, chain: str = 'sol', limit: int = 100, max_initial_quote_reserve: float = 30) -> Optional[List[Dict[str, Any]]]:
        endpoint = f"/pairs/{chain}/new_pairs/24h"
        params = {
            "limit": limit,
            "orderby": "open_timestamp",
            "direction": "desc",
            "period": "24h",
            "filters[]": ["not_honeypot", "renounced", "frozen"],
            "min_marketcap": 100000,
            "min_swaps24h": 10,
            "platforms[]": ["pump", "moonshot", "raydium"],
            "min_holder_count": 15,
            "min_created": "0.6m",
            "max_created": "30m"
        }
        response = await self._make_request(endpoint, params=params)
        if response and 'pairs' in response:
            return [
                pair for pair in response['pairs']
                if float(pair.get('initial_quote_reserve', '0')) < max_initial_quote_reserve
            ][:limit]
        return None

    async def get_wallet_data(self, address: str, chain: str = 'sol', period: str = '7d') -> Optional[Dict[str, Any]]:
        endpoint = f"/smartmoney/{chain}/walletNew/{address}"
        response = await self._make_request(endpoint, params={"period": period})
        return response.get('data') if response else None

    async def get_token_info(self, token_address: str, chain: str = 'sol') -> Optional[Dict[str, Any]]:
        chain_info = get_chain_info(chain)
        chain_slug = chain_info.shortName
        endpoint = f"/tokens/{chain_slug}/{token_address}"
        response = await self._make_request(endpoint)
        if not response or "data" not in response:
            return None
            
        token_data = response.get('data', {}).get('token', {})
        # print(token_data)
        if not token_data:
            return None
            
        return self._convert_to_token(token_data)

    async def get_wallet_holdings(self, wallet_address: str, chain: str = 'sol', order_by: str = 'last_active_timestamp', direction: str = 'desc', show_small: bool = True, sellout: bool = True, limit: int = 50, tx30d: bool = True) -> Optional[List[Dict[str, Any]]]:
        chain = 'sol' if chain == 'solana' else chain
        endpoint = f"/wallet/{chain}/holdings/{wallet_address}"
        params = {
            "orderby": order_by,
            "direction": direction,
            "showsmall": str(show_small).lower(),
            "sellout": str(sellout).lower(),
            "limit": limit,
            "tx30d": str(tx30d).lower()
        }
        response = await self._make_request(endpoint, params=params)
        return response.get('data', {}).get('holdings') if response else None


    def _convert_to_token(self, data: Dict[str, Any]) -> Token:
        return Token.from_gmgn(data)

    def _convert_to_holder(self, data: Dict[str, Any], token_address: str) -> Holder:
        return Holder.from_gmgn(data, token_address)
    
    # Example of a method to get multiple tokens concurrently
    async def get_multiple_tokens(self, token_addresses: List[str], chain: str = 'sol') -> List[Optional[Dict[str, Any]]]:
        tasks = [self.get_token_info(address, chain) for address in token_addresses]
        return await asyncio.gather(*tasks)