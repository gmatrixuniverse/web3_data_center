from typing import List, Dict, Any
from .base_client import BaseClient

class DexScreenerClient(BaseClient):
    def __init__(self, config_path: str = "config.yml", use_proxy: bool = True):
        super().__init__('dexscreener', config_path=config_path, use_proxy=use_proxy)

    async def fetch_token_info(self, addresses: List[str]) -> Dict[str, Any]:
        if not addresses:
            return {}

        endpoint = f"/dex/tokens/{','.join(addresses)}"
        return await self._make_request(endpoint)

    async def search_pairs(self, query: str) -> Dict[str, Any]:
        endpoint = "/dex/search"
        params = {"q": query}
        return await self._make_request(endpoint, params=params)

    def process_token_info(self, token_info: Dict[str, Any]) -> Dict[str, Any]:
        processed_info = {}
        if 'pairs' in token_info:
            for pair in token_info['pairs']:
                base_token = pair['baseToken']
                quote_token = pair['quoteToken']
                address = base_token['address']
                volume_24h = float(pair['volume']['h24'])
                liquidity_usd = float(pair['liquidity']['usd'])
                if volume_24h >= 1000 and liquidity_usd >= 1000:
                    pair_address = pair['pairAddress']
                    if pair_address not in processed_info:
                        processed_info[pair_address] = {
                            'base_token': {
                                'address': address,
                                'name': base_token['name'],
                                'symbol': base_token['symbol']
                            },
                            'quote_token': {
                                'address': quote_token['address'],
                                'name': quote_token['name'],
                                'symbol': quote_token['symbol']
                            },
                            'price_usd': pair['priceUsd'],
                            'volume_24h': volume_24h,
                            'liquidity_usd': liquidity_usd,
                            'dex_id': pair['dexId'],
                            'chain_id': pair['chainId']
                        }
        return processed_info

    def process_search_results(self, search_results: Dict[str, Any]) -> Dict[str, Any]:
        processed_results = {}
        # print("Search Result: ",search_results)
        if 'pairs' in search_results:
            for pair in search_results['pairs']:
                volume_24h = float(pair['volume']['h24'])
                liquidity_usd = float(pair['liquidity']['usd'])
                if volume_24h >= 1000 and liquidity_usd >= 1000:
                    pair_address = pair['pairAddress']
                    processed_results[pair_address] = {
                        'base_token': {
                            'address': pair['baseToken']['address'],
                            'name': pair['baseToken']['name'],
                            'symbol': pair['baseToken']['symbol']
                        },
                        'quote_token': {
                            'address': pair['quoteToken']['address'],
                            'name': pair['quoteToken']['name'],
                            'symbol': pair['quoteToken']['symbol']
                        },
                        'price_usd': pair['priceUsd'],
                        'volume_24h': volume_24h,
                        'liquidity_usd': liquidity_usd,
                        'dex_id': pair['dexId'],
                        'chain_id': pair['chainId']
                    }
        return processed_results

    async def get_processed_token_info(self, addresses: List[str]) -> Dict[str, Any]:
        raw_info = await self.fetch_token_info(addresses)
        return self.process_token_info(raw_info)

    async def get_processed_search_results(self, query: str) -> Dict[str, Any]:
        raw_results = await self.search_pairs(query)
        return self.process_search_results(raw_results)