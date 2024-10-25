from typing import Dict, Any, Optional, List
from datetime import datetime
import csv
from .base_client import BaseClient
from dataclasses import dataclass
from ..models.token import Token
from ..models.holder import Holder


@dataclass
class PoolInfo:
    address: str
    name: str
    price_usd: float
    liquidity_usd: float
    volume_usd_24h: float
    price_change_24h: str
    swap_count_24h: int
    created_at: datetime
    tokens: List[Dict[str, Any]]

@dataclass
class RelatedPool:
    address: str
    name: str
    liquidity: float
    volume_usd_24h: float
    price_usd_24h: float
    price_change_24h: str
    dex: Dict[str, Any]
    tokens: List[Dict[str, Any]]

class GeckoTerminalClient(BaseClient):
    def __init__(self, config_path: str = "config.yml", use_proxy: bool = False, use_zenrows: bool = True):
        super().__init__('geckoterminal', config_path=config_path, use_proxy=use_proxy, use_zenrows=use_zenrows)
        self.headers.update({
            'accept': 'application/json',
            'user-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36'
        })
        
    async def get_pool_info(self, pool_address: str, network: str = 'solana') -> Optional[PoolInfo]:
        endpoint = f"/api/p1/{network}/pools/{pool_address}"
        params = {
            "include": "dex,dex.network.explorers,dex_link_services,network_link_services,pairs,token_link_services,tokens.token_security_metric,tokens.tags",
            "base_token": "0"
        }
        response = await self._make_request(endpoint, params=params)
        
        if response and 'data' in response:
            data = response['data']['attributes']
            return PoolInfo(
                address=data['address'],
                name=data['name'],
                price_usd=float(data['price_in_usd']),
                liquidity_usd=float(data['reserve_in_usd']),
                volume_usd_24h=float(data['from_volume_in_usd']),
                price_change_24h=data['price_percent_change'],
                swap_count_24h=data['swap_count_24h'],
                created_at=datetime.fromisoformat(data['pool_created_at']),
                tokens=[token['attributes'] for token in response['included'] if token['type'] == 'token']
            )
        return None

    async def get_hot_tokens(self, 
                       limit: int = 10, 
                       volume_24h_range: tuple = (1, 100000000),
                       liquidity_range: tuple = (1, 100000000),
                       fdv_usd_range: tuple = (1, 100000000),
                       tx_count_24h_range: tuple = (0, 10000000),
                       buys_24h_range: tuple = (0, 10000000),
                       sells_24h_range: tuple = (0, 10000000)) -> List[Token]:
        url = f"/pools"
        params = {
            "include": "dex,dex.network,tokens",
            "include_network_metrics": "true",
            "sort": "-24h_trend_score",
            "page": 1,
            "volume_24h[gte]": volume_24h_range[0],
            "volume_24h[lte]": volume_24h_range[1],
            "liquidity[gte]": liquidity_range[0],
            "liquidity[lte]": liquidity_range[1],
            "fdv_in_usd[gte]": fdv_usd_range[0],
            "fdv_in_usd[lte]": fdv_usd_range[1],
            "tx_count_24h[gte]": tx_count_24h_range[0],
            "tx_count_24h[lte]": tx_count_24h_range[1],
            "buys_24h[gte]": buys_24h_range[0],
            "buys_24h[lte]": buys_24h_range[1],
            "sells_24h[gte]": sells_24h_range[0],
            "sells_24h[lte]": sells_24h_range[1],
            "networks": "solana"
        }

        response = await self._make_request(url, params=params)

        data = response

        tokens = []
        for pool in data.get("data", [])[:limit]:
            token = self._parse_pool_data(pool, data["included"])
            if token:
                tokens.append(token)

        return tokens

    def _parse_pool_data(self, pool: Dict[str, Any], included: List[Dict[str, Any]]) -> Optional[Token]:
        attributes = pool["attributes"]
        relationships = pool["relationships"]

        base_token_id = attributes.get("base_token_id")
        base_token = next((t for t in included if t["type"] == "token" and t["id"] == base_token_id), None)
        
        if not base_token:
            return None

        dex = next((d for d in included if d["type"] == "dex" and d["id"] == relationships["dex"]["data"]["id"]), None)
        network = next((n for n in included if n["type"] == "network" and n["id"] == dex["relationships"]["network"]["data"]["id"]), None)

        return Token(
            name=base_token["attributes"]["name"],
            symbol=base_token["attributes"]["symbol"],
            address=base_token["attributes"]["address"],
            price=float(attributes.get("price_in_usd", 0)),
        )

    async def get_price_history(self, token_address: str, chain: str = 'solana', interval: str = '1h', limit: int = 1000) -> Optional[List[Dict[str, Any]]]:
        endpoint = f"/tokens/{chain}/{token_address}/ohlcv/{interval}"
        params = {"limit": limit}
        return await self._make_request(endpoint, params=params)

    async def get_top_holders(self, token_address: str, limit: int = 20, chain: str = 'solana') -> Optional[List[Dict[str, Any]]]:
        endpoint = f"/tokens/{chain}/{token_address}/holders"
        params = {
            "page": 1,
            "limit": limit
        }
        return await self._make_request(endpoint, params=params)