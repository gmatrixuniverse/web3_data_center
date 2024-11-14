import asyncio
from typing import Dict, List, Optional, Any
from ..clients import *
from ..models.token import Token
from ..models.holder import Holder
from ..models.price_history_point import PriceHistoryPoint
from ..utils.logger import get_logger
import time
import datetime
from chain_index import get_chain_info, get_all_chain_tokens
from evm_decoder.utils.abi_utils import is_pair_swap
from evm_decoder.utils.constants import UNI_V2_SWAP_TOPIC, UNI_V3_SWAP_TOPIC
from evm_decoder import DecoderManager, AnalyzerManager, ContractManager
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type
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
        self.etherscan_client = EtherscanClient(config_path=config_path)
        self.opensearch_client = OpenSearchClient(config_path=config_path)
        self.w3_client = Web3(Web3.HTTPProvider("http://192.168.0.105:8545"))
        self.contract_manager = ContractManager("http://192.168.0.105:8545")
        self.analyzer  = AnalyzerManager()
        self.decoder = DecoderManager()

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

            # logger.info(f"Called price: {called_price}")
            
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

    async def get_deployed_contracts(self, address: str, chain: str = 'eth') -> Optional[List[Dict[str, Any]]]:
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

    async def get_deployed_block(self, address: str, chain: str = 'eth') -> Optional[int]:
        cache_key = f"deployed_block:{chain}:{address}"
        if cache_key in self.cache:
            return self.cache[cache_key]
        
        try:
            deployment = await self.etherscan_client.get_deployment(address)
            deployed_tx = deployment['txHash']
            tx = self.w3_client.eth.get_transaction(deployed_tx)
            deployed_block = tx['blockNumber']

            self.cache[cache_key] = deployed_block
            return deployed_block
        except Exception as e:
            logger.error(f"Error fetching deployed block for {address}: {str(e)}")
            return None



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

    async def has_code(self, address: str, chain: str = 'eth') -> bool:
        return self.w3_client.eth.get_code(address) != b''

    async def calculate_all_pair_addresses(self, token_contract: str, chain: str = 'eth'):
        w3 = Web3()
        tokens = get_all_chain_tokens(chain).get_all_tokens()
        pair_addresses = []
        for token_symbol, token_info in tokens.items():
            for dex_type in ['uniswap_v2', 'uniswap_v3']:
                pair_address = await self.calculate_pair_address(token_contract, token_info.contract, dex_type)   
                if await self.has_code(pair_address):
                    pair_addresses.append({
                        'dex_type': dex_type,
                        'pair_address': pair_address
                    })
        return pair_addresses

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

    async def get_token_pair_orders_at_block(self, token_contract: str, pair_address: str, block_number: int = -1, chain: str = 'eth') -> List[Dict[str, Any]]:
        swap_orders = []
        try:
            chain_obj = get_chain_info(chain)
            if chain_obj.chainId == 1:
                logs = await self.get_logs_at_block(block_number, chain, [pair_address])
                for log in logs:
                    if is_pair_swap(log):
                        orders = self.reconstruct_order_from_log(log,token_contract)
                        swap_orders.append(orders)
                return swap_orders
            elif chain_obj.chainId == 137:
                logs = []
                return logs
            else:
                raise ValueError(f"Unsupported chain: {chain}")
                
        except Exception as e:
            logger.error(f"Error getting token pair orders at block: {str(e)}")
            return []

    async def get_token_pair_orders_between(self, token_contract: str, pair_address: str, block_start: int = 0, block_end: int = 99999999, chain: str = 'eth') -> List[Dict[str, Any]]:
        swap_orders = []
        # logger.info(f"getting token pair orders between {block_start} and {block_end}")
        try:
            chain_obj = get_chain_info(chain)
            if chain_obj.chainId == 1:
                logs = self.w3_client.eth.get_logs({
                    'fromBlock': block_start,
                    'toBlock': block_end,
                    'address': self.w3_client.toChecksumAddress(pair_address),
                    'topics': [
                        [UNI_V2_SWAP_TOPIC, UNI_V3_SWAP_TOPIC]
                    ]
                })
                # logger.info(logs)
                swap_orders = await self.reconstruct_orders_from_logs(logs,token_contract)
                return swap_orders
            elif chain_obj.chainId == 137:
                logs = []
                return logs
            else:
                raise ValueError(f"Unsupported chain: {chain}")
                
        except Exception as e:
            logger.error(f"Error getting latest pair orders: {str(e)}")
            return []

    async def reconstruct_orders_from_logs(self, logs: List[Dict[str, Any]], token_contract: str) -> List[Dict[str, Any]]:
        try:
            orders = []
            for log in logs:
                order = await self.reconstruct_order_from_log(log, token_contract)
                orders.append(order)
            return orders
        except Exception as e:
            logger.error(f"Error reconstructing order from log: {str(e)}")
            return None

    async def reconstruct_order_from_log(self, log: Dict[str, Any], token_contract: str) -> Dict[str, Any]:
        try:
            # logger.info(f"reconstructing order from log + {log}")
            tx = await self.get_tx_with_logs_by_log(log)
            # logger.info(f"here is tx: {tx}")

            analysis = self.analyzer.analyze_transaction(tx)
            pair = log['address'].lower()
            side = "Sell" if analysis['balance_analysis'][pair][token_contract.lower()] > 0 else "Buy"
            token_amount = abs(analysis['balance_analysis'][pair][token_contract.lower()])
            native_token_amount = abs(analysis['balance_analysis'][pair]['native'])
            if side == "Buy":
                token_balances = {
                    addr: balances.get(token_contract.lower(), 0)
                    for addr, balances in analysis['balance_analysis'].items()
                }
                max_token_amount = max(token_balances.values())
                addresses_with_max = [
                    addr for addr, amount in token_balances.items()
                    if amount == max_token_amount
                ]
                
                if tx['from'].lower() in addresses_with_max:
                    receiver = tx['from']
                elif tx['to'].lower() in addresses_with_max:
                    receiver = tx['to']
                else:
                    receiver = addresses_with_max[0]
                
            else:
                # to know receiver in sell circumstance, we need to find the address with the largest native addition
                token_balances = {
                    addr: balances.get('native', 0)
                    for addr, balances in analysis['balance_analysis'].items()
                }
                max_token_amount = max(token_balances.values())
                addresses_with_max = [
                    addr for addr, amount in token_balances.items() 
                    if amount == max_token_amount
                ]
                if tx['from'].lower() in addresses_with_max:
                    receiver = tx['from']
                elif tx['to'].lower() in addresses_with_max:
                    receiver = tx['to']
                else:
                    receiver = addresses_with_max[0]
            # print("check tx", tx)
            order = {
                'timestamp': int(tx['blockTimestamp'], 16),
                'trader': tx['from'],
                'receiver': receiver,
                'token': token_contract,
                'side': side,
                'token_amount': token_amount,
                'native_token_amount': native_token_amount,
                'price': token_amount / native_token_amount if native_token_amount != 0 else 0,
                'volumeUSD': native_token_amount,
                'platform': tx['to'],
                'transaction_hash': tx['hash']
            }
            return order
        except Exception as e:
            logger.error(f"Error reconstructing order from log: {str(e)}")
            return None

    async def get_tx_with_logs_by_hash(self, tx_hash: str, return_dict: bool = True) -> Dict[str, Any]:
        try:
            # logger.info(f"getting tx with logs by hash: {tx_hash}")

            if isinstance(tx_hash, bytes):
                tx_hash = tx_hash.hex()
            tx = self.w3_client.eth.get_transaction(tx_hash)
            receipt = self.w3_client.eth.get_transaction_receipt(tx_hash)

            if tx is None or receipt is None:
                logger.error("Transaction or receipt not found")
                raise ValueError("Transaction or receipt not found")

            tx_dict = dict(tx)
            for key, value in tx_dict.items():
                if hasattr(value, 'hex'):
                    tx_dict[key] = value.hex()

            # add a blockTimestamp from any log to tx_dict
            for log in receipt.logs:
                if 'blockTimestamp' in log:
                    tx_dict['blockTimestamp'] = log['blockTimestamp']
                    break

            logs = []
            for log in receipt.logs:
                log_dict = dict(log)
                for key, value in log_dict.items():
                    if hasattr(value, 'hex'):
                        log_dict[key] = value.hex()
                    elif isinstance(value, list):
                        log_dict[key] = [
                            item.hex() if hasattr(item, 'hex') else item
                            for item in value
                        ]
                logs.append(log_dict)

            tx_dict['logs'] = logs
            return tx_dict

        except Exception as e:
            logger.error(f"Error fetching tx with logs: {e}")
            raise  # 抛出异常以触发重试

    async def get_tx_by_log(self, log: Dict[str, Any], token_contract: str, chain: str = 'eth') -> Dict[str, Any]:
        try:
            tx = await self.get_tx_by_hash(log['transactionHash'])
            return tx
        except Exception as e:
            logger.error(f"Error getting tx with logs by log: {str(e)}")
            return None

    async def is_pair_rugged(self, pair_address: str, pair_type: str = 'uniswap_v2', chain: str = 'eth') -> bool:
        try:
            # use web3 to check if the pair's reserve is rugged
            if pair_type == 'uniswap_v2':
                reserves = self.contract_manager.read_contract(
                    contract_type=pair_type,
                    address=pair_address,
                    method='getReserves'
                )
                token0 = self.contract_manager.read_contract(
                    contract_type=pair_type,
                    address=pair_address,
                    method='token0'
                )
                token1 = self.contract_manager.read_contract(
                    contract_type=pair_type,
                    address=pair_address,
                    method='token1'
                )
                alternative_tokens = get_all_chain_tokens(chain).get_all_tokens()
                # calculate value if token0(token1) is alternative token
                token0_value = 0
                token1_value = 0
                
                # Find matching alternative token by contract address
                for alt_token in alternative_tokens.values():
                    if token0.lower() == alt_token.contract.lower():
                        token0_value = reserves[0] / 10 ** alt_token.decimals * alt_token.price_usd
                    if token1.lower() == alt_token.contract.lower():
                        token1_value = reserves[1] / 10 ** alt_token.decimals * alt_token.price_usd

                # logger.info(f"token0_value: {token0_value}, token1_value: {token1_value}")
                return token0_value + token1_value < 100

            elif pair_type == 'uniswap_v3':
                liquidity = self.contract_manager.read_contract(
                    contract_type=pair_type,
                    address=pair_address,
                    method='liquidity'
                )
                return liquidity == 0
            return False
        except Exception as e:
            logger.error(f"Error checking pair rugged: {str(e)}")
            return None

    async def is_token_rugged(self, token_contract: str, chain: str = '1') -> bool:
        try:
            # use web3 to check if the pair's reserve is rugged
            pair_address_list = await self.calculate_all_pair_addresses(token_contract, chain)
            for pair_address in pair_address_list:
                if await self.is_pair_rugged(pair_address['pair_address'], pair_address['dex_type'], chain):
                    return True
            return False
        except Exception as e:
            logger.error(f"Error checking token rugged: {str(e)}")
            return None

    async def get_tx_with_logs_by_log(self, log: Dict[str, Any], chain: str = 'eth') -> Dict[str, Any]:
        try:
            # logger.info("getting tx with logs by log")
            result = await self.get_tx_with_logs_by_hash(log['transactionHash'])
            # logger.info(result)
            return result

        except Exception as e:
            logger.error(f"Error getting tx with logs by log: {str(e)}")
            return None

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


    async def get_txs_with_logs_at_block(self, block_number: int = -1, chain: str = 'eth') -> List[Dict[str, Any]]:
        try:
            chain_obj = get_chain_info(chain)
            if chain_obj.chainId == 1:
                # Use loop.run_in_executor for blocking Web3 calls
                loop = asyncio.get_event_loop()
                
                # Get transactions and logs concurrently
                block = await loop.run_in_executor(None, lambda: self.w3_client.eth.get_block(block_number, full_transactions=True))
                logs = await loop.run_in_executor(None, lambda: self.w3_client.eth.get_logs({
                    'fromBlock': block_number if block_number != -1 else "latest",
                    'toBlock': block_number if block_number != -1 else "latest"
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
            logger.error(f"Error in get_txs_with_logs_at_block: {str(e)}")
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