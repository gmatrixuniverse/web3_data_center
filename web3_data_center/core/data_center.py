import asyncio
from typing import Dict, List, Optional, Any
from ..clients.geckoterminal_client import GeckoTerminalClient
from ..clients.gmgn_api_client import GMGNAPIClient
from ..clients.birdeye_client import BirdeyeClient
from ..clients.solscan_client import SolscanClient
from ..clients.opensearch_client import OpenSearchClient
from ..models.token import Token
from ..models.holder import Holder
from ..models.price_history_point import PriceHistoryPoint
from ..utils.logger import get_logger
import time
import datetime
from chain_index import get_chain_info

logger = get_logger(__name__)

class DataCenter:
    def __init__(self, config_path: str = "config.yml"):
        self.geckoterminal_client = GeckoTerminalClient(config_path=config_path)
        self.gmgn_client = GMGNAPIClient(config_path=config_path)
        self.birdeye_client = BirdeyeClient(config_path=config_path)
        self.solscan_client = SolscanClient(config_path=config_path)
        # self.opensearch_client = OpenSearchClient(config_path=config_path)
        self.cache = {}

    async def get_token_call_performance(self, address: str, called_time: datetime.datetime, chain: str = 'sol') -> Optional[Token]:
        price_history = await self.get_token_price_history(address, chain, resolution='1m', from_time=int(called_time.timestamp()), to_time=int(time.time()))
        # calculate drawdown and ath multiple since called_time
        # price at called_time
        # get correct place from price_history like: [{'open': '0.78457552178042790000', 'high': '0.78457552178042790000', 'low': '0.74969875041493808885', 'close': '0.76926061486827099588', 'volume': '327317.73133496651430000000', 'time': '1729776600000'}, {'open': '0.76926061486827099588', 'high': '0.77963204815911584348', 'low': '0.76476126987210720000', 'close': '0.77317198902943750000', 'volume': '280574.22261012006010000000', 'time': '1729776660000'},
        called_price = float(price_history[0]['close'])
        # called_price = self.get_token_price_at_time(address, chain, called_time)
        logger.info(f"Called price: {called_price}")
        max_price = called_price
        max_price_timestamp = None
        min_price = called_price
        min_price_timestamp = None
        for price_point in price_history:
            price_point_time = datetime.datetime.fromtimestamp(int(price_point['time'])/1000)  # Assuming 'time' is in milliseconds
            if price_point_time > datetime.datetime.now():
                break
            if float(price_point['close']) > max_price:
                max_price = float(price_point['close'])
                max_price_timestamp = price_point['time']
            if float(price_point['close']) < min_price:
                min_price = float(price_point['close'])
                min_price_timestamp = price_point['time']
        logger.info(f"Max price: {max_price}, Max price timestamp: {max_price_timestamp}, Min price: {min_price}, Min price timestamp: {min_price_timestamp}")
        drawdown = min_price / called_price - 1 if called_price > min_price else 0
        ath_multiple = max_price / called_price
        return ath_multiple, drawdown

    async def get_token_price_at_time(self, address: str, chain: str = 'sol') -> Optional[Token]:
        cache_key = f"token_info:{chain}:{address}"
        if cache_key in self.cache:
            return self.cache[cache_key]

        token = await self.birdeye_client.get_token_price_at_time(address, chain)

        if token:
            self.cache[cache_key] = token
        return token

    async def get_token_info(self, address: str, chain: str = 'sol') -> Optional[Token]:
        cache_key = f"token_info:{chain}:{address}"
        if cache_key in self.cache:
            return self.cache[cache_key]

        token = None
        if chain == 'solana':
            token = await self.solscan_client.get_token_info(address)
            if not token:
                token = await self.birdeye_client.get_token_info(address)
        else:
            # Implement for other chains if needed
            pass

        if token:
            self.cache[cache_key] = token
        return token

    async def get_price_history(self, address: str, chain: str = 'solana', interval: str = '15m', limit: int = 1000) -> List[PriceHistoryPoint]:
        cache_key = f"price_history:{chain}:{address}:{interval}:{limit}"
        if cache_key in self.cache:
            return self.cache[cache_key]

        price_history = await self.birdeye_client.get_price_history(address, interval=interval, max_records=limit)
        self.cache[cache_key] = price_history
        return price_history

    async def get_top_holders(self, address: str, chain: str = 'solana', limit: int = 20) -> List[Holder]:
        cache_key = f"top_holders:{chain}:{address}:{limit}"
        if cache_key in self.cache:
            return self.cache[cache_key]

        holders = await self.solscan_client.get_top_holders(address, page_size=limit)
        if not holders:
            holders = await self.birdeye_client.get_all_top_traders(address, max_traders=limit)

        self.cache[cache_key] = holders
        return holders

    async def get_hot_tokens(self, chain: str = 'solana', limit: int = 100) -> List[Token]:
        cache_key = f"hot_tokens:{chain}:{limit}"
        if cache_key in self.cache:
            return self.cache[cache_key]

        hot_tokens = await self.gmgn_client.get_token_list(chain, limit=limit)
        self.cache[cache_key] = hot_tokens
        return hot_tokens

    async def search_logs(self, index: str, start_block: int, end_block: int, event_topics: List[str], size: int = 1000) -> List[Dict[str, Any]]:
        cache_key = f"logs:{index}:{start_block}:{end_block}:{':'.join(event_topics)}:{size}"
        if cache_key in self.cache:
            return self.cache[cache_key]

        logs = await self.opensearch_client.search_logs(index, start_block, end_block, event_topics, size)
        self.cache[cache_key] = logs
        return logs

    async def get_blocks_brief(self, start_block: int, end_block: int, size: int = 1000) -> List[Dict[str, Any]]:
        cache_key = f"blocks_brief:{start_block}:{end_block}"
        cached_result = self.get_cache_item(cache_key)
        if cached_result:
            return cached_result

        blocks = await self.opensearch_client.get_blocks_brief(start_block, end_block, size)
        
        # Only cache if the result is not too large
        if len(blocks) <= 10000:  # Adjust this threshold as needed
            self.set_cache_item(cache_key, blocks)
        
        return blocks

    async def get_token_price_history(self, token_address: str, chain: str = 'eth', resolution: str = '1m', from_time: int = None, to_time: int = None) -> Optional[List[Dict[str, Any]]]:
        cache_key = f"token_price_history:{chain}:{token_address}:{resolution}:{from_time}:{to_time}"
        # logger.info(f"Getting token price history for {chain}:{token_address} with resolution {resolution} from {from_time} to {to_time}")
        if cache_key in self.cache:
            return self.cache[cache_key]

        price_history = await self.gmgn_client.get_token_price_history(token_address, chain, resolution, from_time, to_time)
        # logger.info(f"Got token price history for {token_address}: {price_history}")
        self.cache[cache_key] = price_history['data']
        return price_history['data']

    async def get_new_pairs(self, chain: str = 'sol', limit: int = 100, max_initial_quote_reserve: float = 30) -> Optional[List[Dict[str, Any]]]:
        cache_key = f"new_pairs:{chain}:{limit}:{max_initial_quote_reserve}"
        if cache_key in self.cache:
            return self.cache[cache_key]

        new_pairs = await self.gmgn_client.get_new_pairs(chain, limit, max_initial_quote_reserve)
        self.cache[cache_key] = new_pairs
        return new_pairs

    async def get_wallet_data(self, address: str, chain: str = 'sol', period: str = '7d') -> Optional[Dict[str, Any]]:
        cache_key = f"wallet_data:{chain}:{address}:{period}"
        if cache_key in self.cache:
            return self.cache[cache_key]

        wallet_data = await self.gmgn_client.get_wallet_data(address, chain, period)
        self.cache[cache_key] = wallet_data
        return wallet_data

    def clear_cache(self):
        self.cache.clear()

    def set_cache_item(self, key: str, value: Any, expiration: int = 3600):
        self.cache[key] = {
            'value': value,
            'expiration': time.time() + expiration
        }

    def get_cache_item(self, key: str) -> Optional[Any]:
        if key in self.cache:
            item = self.cache[key]
            if time.time() < item['expiration']:
                return item['value']
            else:
                del self.cache[key]
        return None

    async def close(self):
        # await self.opensearch_client.close()
        pass
        # Close other clients if they have close methods

    async def get_specific_txs(self, to_address: str, start_block: int, end_block: int, size: int = 1000) -> List[Dict[str, Any]]:
        cache_key = f"specific_txs:{to_address}:{start_block}:{end_block}:{size}"
        cached_result = self.get_cache_item(cache_key)
        if cached_result is not None:
            logger.warning(f"Returning cached result for {cache_key}")
            return cached_result

        logger.info(f"Fetching transactions for address {to_address} from block {start_block} to {end_block}")
        try:
            transactions = await self.opensearch_client.get_specific_txs(to_address, start_block, end_block, size)
            logger.info(f"Retrieved {len(transactions)} transactions for address {to_address}")

            if transactions:
                min_block = min(tx['block_number'] for tx in transactions)
                max_block = max(tx['block_number'] for tx in transactions)
                logger.info(f"Transaction block range: {min_block} to {max_block}")

            if len(transactions) <= 10000:  # Adjust this threshold as needed
                self.set_cache_item(cache_key, transactions)
                logger.info(f"Cached {len(transactions)} transactions for key {cache_key}")
            else:
                logger.warning(f"Not caching {len(transactions)} transactions as it exceeds the threshold")

            return transactions
        except Exception as e:
            logger.error(f"Error fetching transactions: {str(e)}")
            return []

    async def get_specific_txs_batched(self, to_address: str, start_block: int, end_block: int, size: int = 1000) -> List[Dict[str, Any]]:
        cache_key = f"specific_txs_batch:{to_address}:{start_block}:{end_block}:{size}"
        cached_result = self.get_cache_item(cache_key)
        if cached_result is not None:
            logger.warning(f"Returning cached result for {cache_key}")
            yield cached_result
            return

        logger.info(f"Fetching transactions for address {to_address} from block {start_block} to {end_block}")
        try:
            total_transactions = 0
            min_block = float('inf')
            max_block = float(0)

            async for batch in self.opensearch_client.get_specific_txs_batched(to_address, start_block, end_block, size):
                total_transactions += len(batch)
                if batch:
                    min_block = min(min_block, min(tx['block_number'] for tx in batch))
                    max_block = max(max_block, max(tx['block_number'] for tx in batch))
                
                yield batch

            logger.info(f"Retrieved {total_transactions} transactions for address {to_address}")
            if total_transactions > 0:
                logger.info(f"Transaction block range: {min_block} to {max_block}")

            # if total_transactions <= 500:  # Adjust this threshold as needed
            #     logger.info(f"Caching {total_transactions} transactions for key {cache_key}")
            #     # Note: Caching logic might need to be adjusted for batch processing
            # else:
            #     logger.warning(f"Not caching {total_transactions} transactions as it exceeds the threshold")

        except Exception as e:
            logger.error(f"Error fetching transactions: {str(e)}")
            yield []
