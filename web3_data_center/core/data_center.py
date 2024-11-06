import asyncio
from typing import Dict, List, Optional, Any
from ..clients.geckoterminal_client import GeckoTerminalClient
from ..clients.gmgn_api_client import GMGNAPIClient
from ..clients.birdeye_client import BirdeyeClient
from ..clients.solscan_client import SolscanClient
from ..clients.dexscreener_client import DexScreenerClient
from ..clients.goplus_client import GoPlusClient
from ..clients.opensearch_client import OpenSearchClient
from ..clients.chainbase_client import ChainbaseClient
from ..models.token import Token
from ..models.holder import Holder
from ..models.price_history_point import PriceHistoryPoint
from ..utils.logger import get_logger
import time
import datetime
from chain_index import get_chain_info
from web3 import Web3
logger = get_logger(__name__)

class DataCenter:
    def __init__(self, config_path: str = "config.yml"):
        self.geckoterminal_client = GeckoTerminalClient(config_path=config_path)
        self.gmgn_client = GMGNAPIClient(config_path=config_path)
        self.birdeye_client = BirdeyeClient(config_path=config_path)
        self.solscan_client = SolscanClient(config_path=config_path)
        self.goplus_client = GoPlusClient(config_path=config_path)
        self.dexscreener_client = DexScreenerClient(config_path=config_path)
        self.chainbase_client = ChainbaseClient(config_path=config_path)
        self.w3_client = Web3(Web3.HTTPProvider("http://192.168.0.105:8545"))
        # self.opensearch_client = OpenSearchClient(config_path=config_path)
        self.cache = {}

    async def get_token_call_performance(self, address: str, called_time: datetime.datetime, chain: str = 'sol') -> Optional[tuple[str, float, float]]:
        try:
            # Get token info with validation
            info = await self.get_token_info(address, chain)
            if not info or not info.symbol:
                logger.error(f"Failed to get token info for {address} on {chain}")
                return None
            
            # Get price history with validation
            price_history = await self.get_token_price_history(
                address, 
                chain, 
                resolution='1m', 
                from_time=int(called_time.timestamp()), 
                to_time=int(time.time())
            )
            
            if not price_history or len(price_history) == 0:
                logger.error(f"No price history available for {address} on {chain}")
                return None
                
            # Get initial price with validation
            try:
                called_price = float(price_history[0]['close'])
                if called_price <= 0:
                    logger.error(f"Invalid called price ({called_price}) for {address}")
                    return None
            except (KeyError, ValueError, IndexError) as e:
                logger.error(f"Error parsing initial price for {address}: {str(e)}")
                return None

            logger.info(f"Called price: {called_price}")
            
            # Track price extremes
            max_price = called_price
            max_price_timestamp = None
            min_price = called_price
            min_price_timestamp = None
            current_time = datetime.datetime.now()
            
            # Process price history
            for price_point in price_history:
                try:
                    # Validate price point data
                    if not all(k in price_point for k in ['time', 'close']):
                        continue
                        
                    price_point_time = datetime.datetime.fromtimestamp(int(price_point['time'])/1000)
                    if price_point_time > current_time:
                        break
                        
                    close_price = float(price_point['close'])
                    if close_price <= 0:
                        continue
                        
                    if close_price > max_price:
                        max_price = close_price
                        max_price_timestamp = price_point['time']
                    if close_price < min_price:
                        min_price = close_price
                        min_price_timestamp = price_point['time']
                        
                except (ValueError, KeyError, TypeError) as e:
                    logger.warning(f"Error processing price point for {address}: {str(e)}")
                    continue

            logger.info(
                f"Max price: {max_price}, Max price timestamp: {max_price_timestamp}, "
                f"Min price: {min_price}, Min price timestamp: {min_price_timestamp}"
            )

            # Calculate performance metrics
            drawdown = min_price / called_price - 1 if called_price > min_price else 0
            ath_multiple = max_price / called_price - 1
            
            return info.symbol, ath_multiple, drawdown
            
        except Exception as e:
            logger.error(f"Error in get_token_call_performance for {address} on {chain}: {str(e)}")
            return None 

    async def get_token_price_at_time(self, address: str, chain: str = 'sol') -> Optional[Token]:
        cache_key = f"token_info:{chain}:{address}"
        if cache_key in self.cache:
            return self.cache[cache_key]

        token = await self.birdeye_client.get_token_price_at_time(address, chain)

        if token:
            self.cache[cache_key] = token
        return token

    async def get_token_info(self, address: str, chain: str = 'solana') -> Optional[Token]:
        cache_key = f"token_info:{chain}:{address}"
        if cache_key in self.cache:
            return self.cache[cache_key]

        token = None
        chaininfo = get_chain_info(chain)
        if chaininfo.chainId == -1:
            # print(f"Token from solscan:")
            token = await self.solscan_client.get_token_info(address)
            if not token:
                token = await self.birdeye_client.get_token_info(address)
                # print(f"Token from birdeye: {token}")
            if not token:
                token = await self.gmgn_client.get_token_info(address, chain)
                # print(f"Token from gmgn: {token}")
        elif chaininfo.chainId == 1:
            token = await self.gmgn_client.get_token_info(address, chain)
            if not token:
                token = await self.dexscreener_client.get_processed_token_info([address])
            # Implement for other chains if needed

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

    async def get_deployed_contracts(self, address: str, chain: str = 'sol') -> Optional[List[Dict[str, Any]]]:
        cache_key = f"deployed_contracts:{chain}:{address}"
        if cache_key in self.cache:
            return self.cache[cache_key]
        chain_obj = get_chain_info(chain)
        try:
            response = await self.chainbase_client.query({
                "query":f"SELECT contract_address\nFROM {chain_obj.icon}.transactions\nWHERE from_address = '{address}'\nAND to_address = ''"
            })
            if response and 'data' in response:
                # Extract contract addresses from the result
                deployed_contracts = [
                    row['contract_address'] 
                    for row in response['data'].get('result', [])
                ]
                self.cache[cache_key] = deployed_contracts
                return deployed_contracts
            return []
        except Exception as e:
            logger.error(f"Error fetching deployed contracts: {str(e)}")
            return []

    async def get_contract_tx_user_count(self, address: str, chain: str = 'sol') -> Optional[Dict[str, Any]]:
        cache_key = f"contract_tx_user_count:{chain}:{address}"
        if cache_key in self.cache:
            return self.cache[cache_key]
        chain_obj = get_chain_info(chain)
        try:
            response = await self.chainbase_client.query({
                "query":f"SELECT count(*) as tx_count, count(DISTINCT from_address) as user_count\nFROM {chain_obj.icon}.transactions\nWHERE to_address = '{address}'"
            })
            if response and 'data' in response:
                user_count = response['data']['result'][0]['user_count']
                tx_count = response['data']['result'][0]['tx_count']
                self.cache[cache_key] = {'user_count': user_count, 'tx_count': tx_count}
                return {'user_count': user_count, 'tx_count': tx_count}
            return {'user_count': 0, 'tx_count': 0}
        except Exception as e:
            logger.error(f"Error fetching contract user count: {str(e)}")
            return {'user_count': 0, 'tx_count': 0}

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

    async def get_token_security(self, address: str, chain: str = 'sol') -> Optional[Dict[str, Any]]:
        cache_key = f"token_security:{chain}:{address}"
        if cache_key in self.cache:
            return self.cache[cache_key]

        token_security = await self.goplus_client.get_tokens_security([address], chain)[0]
        self.cache[cache_key] = token_security
        return token_security


    async def calculate_pair_address(self, tokenA, tokenB, dex_type='uniswap_v2', fee=None):
        w3 = Web3()

        dex_settings = {
            'uniswap_v2': {
                'factory_address': '0x5C69bEe701ef814a2B6a3EDD4B1652CB9cc5aA6f',
                'init_code_hash': '0x96e8ac4277198ff8b6f785478aa9a39f403cb768dd02cbee326c3e7da348845f'
            },
            'uniswap_v3': {
                'factory_address': '0x1F98431c8aD98523631AE4a59f267346ea31F984',
                'init_code_hash': '0xe34f199b19b2b4f47f68442619d555527d244f78a3297ea89325f843f87b8b54'
            },
            'sushiswap_v2': {
                'factory_address': '0xC0AeE478e3658e2610c5F7A4A2E1777cE9e4f2Ac',
                'init_code_hash': '0x96e8ac42782006f8894161745b24916fe9339b629bc3e7ca895b7c575c1d9c77'
            }
        }

        dex = dex_settings.get(dex_type.lower())
        if not dex:
            raise ValueError("Unsupported DEX type")

        if tokenA > tokenB:
            tokenA, tokenB = tokenB, tokenA

        if dex_type.lower() == 'uniswap_v3' and fee is not None:
            salt = Web3.keccak(
                w3.codec.encode(['address', 'address', 'uint24'],
                            [Web3.toChecksumAddress(tokenA),
                            Web3.toChecksumAddress(tokenB),
                            fee])
            )
        else:
            salt = Web3.keccak(Web3.toBytes(hexstr=tokenA) + Web3.toBytes(hexstr=tokenB))

        pair_address = w3.solidityKeccak(
            ['bytes', 'address', 'bytes32', 'bytes32'],
            [
                '0xff',
                dex['factory_address'],
                salt,
                dex['init_code_hash']
            ]
        )[-20:]

        return w3.toChecksumAddress(pair_address)

    async def check_tokens_safe(self, address_list: List[str], chain: str = 'sol') -> List[bool]:
        chain_obj = get_chain_info(chain)
        return await self.goplus_client.check_tokens_safe(chain_id=chain_obj.chainId, token_address_list=address_list)

    async def get_latest_swap_orders(self, address_list: List[str], chain: str = 'eth') -> List[Dict[str, Any]]:
        try:
            chain_obj = get_chain_info(chain)
            if chain_obj.chainId == 1:
                txs = await self.get_latest_txs_with_logs(address_list, chain)
                return txs
            elif chain_obj.chainId == 137:
                logs = []
                return logs
            else:
                raise ValueError(f"Unsupported chain: {chain}")
                
        except Exception as e:
            logger.error(f"Error getting latest swap orders: {str(e)}")
            return []

    async def get_pairs_info(self, query_string: str) -> List[Dict[str, Any]]:
        try:
            response = await self.dexscreener_client.search_pairs(query_string)
            if response and 'pairs' in response:
                return response['pairs']
            return []
        except Exception as e:
            logger.error(f"Error getting pairs info: {str(e)}")
            return []


    async def get_best_pair(self, contract_address: str) -> List[Dict[str, Any]]:
        try:
            pairs = await self.get_pairs_info(contract_address)
            if len(pairs)>0:
                return pairs[0]
            return None
        except Exception as e:
            logger.error(f"Error getting best pair: {str(e)}")
            return None


    async def get_latest_txs_with_logs(self, address_list: List[str], chain: str = 'eth') -> List[Dict[str, Any]]:
        try:
            chain_obj = get_chain_info(chain)
            if chain_obj.chainId == 1:
                # Use loop.run_in_executor for blocking Web3 calls
                loop = asyncio.get_event_loop()
                
                # Get transactions and logs concurrently
                block = await loop.run_in_executor(None, lambda: self.w3_client.eth.get_block("latest", full_transactions=True))
                logs = await loop.run_in_executor(None, lambda: self.w3_client.eth.get_logs({
                    'fromBlock': "latest",
                    'toBlock': "latest"
                }))
                
                # Create a map of transaction hash to logs
                tx_logs_map = {}
                for log in logs:
                    tx_hash = log['transactionHash'].hex() if isinstance(log['transactionHash'], bytes) else log['transactionHash']
                    if tx_hash not in tx_logs_map:
                        tx_logs_map[tx_hash] = []
                    tx_logs_map[tx_hash].append(log)
                
                # Attach logs to their corresponding transactions
                processed_txs = []
                for tx in block['transactions']:
                    tx_hash = tx['hash'].hex() if isinstance(tx['hash'], bytes) else tx['hash']
                    processed_tx = dict(tx)
                    processed_tx['logs'] = tx_logs_map.get(tx_hash, [])
                    processed_txs.append(processed_tx)
                
                return processed_txs
                
            else:
                raise ValueError(f"Unsupported chain: {chain}")
                
        except Exception as e:
            logger.error(f"Error in get_latest_txs_with_logs: {str(e)}")
            return []

    async def get_latest_swap_txs(self, chain: str = 'ethereum') -> List[Dict[str, Any]]:
        try:
            chain_obj = get_chain_info(chain)
            if chain_obj.chainId == 1:
                # Use loop.run_in_executor for blocking Web3 calls
                loop = asyncio.get_event_loop()
                txs = await loop.run_in_executor(None, lambda: self.w3_client.eth.get_block("latest",full_transactions=True))
                return txs

            elif chain_obj.chainId == 137:
                txs = await loop.run_in_executor(None, lambda: self.w3_client.eth.get_block("latest",full_transactions=True))
                return txs

            else:
                raise ValueError(f"Unsupported chain: {chain}")
                
        except Exception as e:
            logger.error(f"Error getting latest swap orders: {str(e)}")
            return []