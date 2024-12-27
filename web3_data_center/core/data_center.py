import asyncio
from typing import Dict, List, Optional, Any, Union, Tuple
from ..clients import *
from ..models.token import Token
from ..models.holder import Holder
from ..models.price_history_point import PriceHistoryPoint
from ..utils.logger import get_logger
from ..utils.cache import file_cache
import time
import datetime
from chain_index import get_chain_info, get_all_chain_tokens
from evm_decoder.utils.abi_utils import is_pair_swap
from evm_decoder.utils.constants import UNI_V2_SWAP_TOPIC, UNI_V3_SWAP_TOPIC
from evm_decoder import DecoderManager, AnalyzerManager, ContractManager
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type
from web3 import HTTPProvider, Web3
import logging
import random

logger = logging.getLogger(__name__)


TRANSFER_EVENT_TOPIC = "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"

class DataCenter:
    def __init__(self, config_path: str = "config.yml"):
        # Configure logging
        logging.getLogger('web3_data_center.clients.database.postgresql_client').setLevel(logging.WARNING)
        logging.getLogger('web3_data_center.clients.database.web3_label_client').setLevel(logging.WARNING)
        logging.getLogger('web3_data_center.clients.opensearch_client').setLevel(logging.WARNING)
        
        # Configure opensearch library logging
        logging.getLogger('opensearch').setLevel(logging.WARNING)
        logging.getLogger('opensearch.transport').setLevel(logging.WARNING)
        logging.getLogger('opensearch.connection.http_urllib3').setLevel(logging.WARNING)
        
        self._config_path = config_path
        self._clients = {}
        self.cache = {}
        
    def _get_client(self, client_type: str):
        """Get a client instance of the specified type.
        
        Args:
            client_type: Type of client to get ('opensearch', 'postgres', etc.)
        """
        if not hasattr(self, '_clients'):
            self._clients = {}
        if client_type not in self._clients:
            if client_type == 'geckoterminal':
                self._clients[client_type] = GeckoTerminalClient(config_path=self._config_path)
            elif client_type == 'gmgn':
                self._clients[client_type] = GMGNAPIClient(config_path=self._config_path)
            elif client_type == 'birdeye':
                self._clients[client_type] = BirdeyeClient(config_path=self._config_path)
            elif client_type == 'solscan':
                self._clients[client_type] = SolscanClient(config_path=self._config_path)
            elif client_type == 'goplus':
                self._clients[client_type] = GoPlusClient(config_path=self._config_path)
            elif client_type == 'dexscreener':
                self._clients[client_type] = DexScreenerClient(config_path=self._config_path)
            elif client_type == 'chainbase':
                self._clients[client_type] = ChainbaseClient(config_path=self._config_path)
            elif client_type == 'etherscan':
                self._clients[client_type] = EtherscanClient(config_path=self._config_path)
            elif client_type == 'opensearch':
                self._clients[client_type] = OpenSearchClient(config_path=self._config_path)
            elif client_type == 'funding':
                self._clients[client_type] = FundingClient(config_path=self._config_path)
            elif client_type == 'contract_manager':
                self._clients[client_type] = ContractManager("http://192.168.0.105:8545")
            elif client_type == 'analyzer':
                self._clients[client_type] = AnalyzerManager()
            elif client_type == 'decoder':
                self._clients[client_type] = DecoderManager()
            elif client_type == 'label':
                self._clients[client_type] = Web3LabelClient(config_path=self._config_path)
            elif client_type == 'postgres':
                from ..clients.database.postgresql_client import PostgreSQLClient
                self._clients[client_type] = PostgreSQLClient(config_path=self._config_path, db_section='zju')
            elif client_type == 'web3':
                self._clients[client_type] = Web3(HTTPProvider("http://192.168.0.105:8545"))
        return self._clients[client_type]
    
    @property
    def geckoterminal_client(self):
        return self._get_client('geckoterminal')
        
    @property
    def gmgn_client(self):
        return self._get_client('gmgn')
        
    @property
    def birdeye_client(self):
        return self._get_client('birdeye')
        
    @property
    def solscan_client(self):
        return self._get_client('solscan')
        
    @property
    def goplus_client(self):
        return self._get_client('goplus')
        
    @property
    def dexscreener_client(self):
        return self._get_client('dexscreener')
        
    @property
    def chainbase_client(self):
        return self._get_client('chainbase')
        
    @property
    def etherscan_client(self):
        return self._get_client('etherscan')
        
    @property
    def opensearch_client(self):
        return self._get_client('opensearch')
        
    @property
    def funding_client(self):
        return self._get_client('funding')
        
    @property
    def w3_client(self):
        return self._get_client('web3')
        
    @property
    def contract_manager(self):
        return self._get_client('contract_manager')
        
    @property
    def analyzer(self):
        return self._get_client('analyzer')
        
    @property
    def decoder(self):
        return self._get_client('decoder')
        
    @property
    def label_client(self):
        return self._get_client('label')
        
    @property
    def postgres_client(self):
        """Get the PostgreSQL client for token metadata"""
        return self._get_client('postgres')
        
    async def get_address_labels(self, addresses: List[str], chain_id: int = 0) -> List[Dict[str, Any]]:
        """Get labels for a list of addresses"""
        if not self.label_client:
            logger.warning("Web3LabelClient not initialized")
            return []
            
        try:
            return self.label_client.get_addresses_labels(addresses, chain_id)
        except Exception as e:
            logger.error(f"Error getting address labels: {str(e)}")
            return []
            
    async def get_address_by_label(self, label: str, chain_id: int = 0) -> List[Dict[str, Any]]:
        """Find addresses by label"""
        if not self.label_client:
            logger.warning("Web3LabelClient not initialized")
            return []
            
        try:
            return self.label_client.get_addresses_by_label(label, chain_id)
        except Exception as e:
            logger.error(f"Error getting addresses by label: {str(e)}")
            return []
            
    async def get_addresses_by_type(self, type_category: str, chain_id: int = 0) -> List[Dict[str, Any]]:
        """Find addresses by type"""
        if not self.label_client:
            logger.warning("Web3LabelClient not initialized")
            return []
            
        try:
            return self.label_client.get_addresses_by_type(type_category, chain_id)
        except Exception as e:
            logger.error(f"Error getting addresses by type: {str(e)}")
            return []

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

    async def sample_transactions(
        self,
        single_block: Optional[Union[int, str]] = None,
        block_range: Optional[Tuple[int, int]] = None,
        sample_size: int = 100,
        to_address_range: Optional[Tuple[str, str]] = None,
        value_range: Optional[Tuple[int, int]] = None,
        gas_range: Optional[Tuple[int, int]] = None,
        four_bytes_list: Optional[List[str]] = None,
        random_seed: Optional[int] = None,
        full_transactions: int = 0
    ) -> Union[List[str], List[Dict]]:
        """Sample transactions from specified blocks.
        
        Args:
            single_block: Single block to sample from
            block_range: Range of blocks to sample from (inclusive)
            sample_size: Number of transactions to sample
            to_address_range: Range of 'to' addresses to filter
            value_range: Range of transaction values to filter
            gas_range: Range of gas used to filter
            four_bytes_list: List of 4-byte function signatures to filter
            random_seed: Random seed for reproducibility
            full_transactions: Level of transaction detail to return:
                0: Only transaction hashes
                1: Transaction data without logs
                2: Full transaction data with logs
            
        Returns:
            List of transaction hashes if full_transactions=0,
            otherwise list of transaction dictionaries
        """
        if random_seed is not None:
            random.seed(random_seed)
            
        filtered_txs = []
        
        # If single block specified, use that
        if single_block is not None:
            filtered_txs.extend(
                self._fetch_and_filter_block(
                    block_identifier=single_block,
                    to_addr_range=to_address_range,
                    value_range=value_range,
                    gas_range=gas_range,
                    four_bytes_list=four_bytes_list,
                    full_transactions=full_transactions
                )
            )
            
        # Otherwise use block range
        elif block_range is not None:
            start_block, end_block = block_range
            
            # Calculate how many blocks we need to sample to get enough transactions
            # Assuming average of 200 transactions per block
            avg_txs_per_block = 200
            num_blocks_needed = max(1, sample_size // avg_txs_per_block * 2)  # Double for safety
            
            # Sample blocks within range
            block_numbers = range(start_block, end_block + 1)
            sampled_blocks = random.sample(block_numbers, min(num_blocks_needed, len(block_numbers)))
            
            # Fetch transactions from sampled blocks
            for blk_num in sampled_blocks:
                filtered_txs.extend(
                    self._fetch_and_filter_block(
                        block_identifier=blk_num,
                        to_addr_range=to_address_range,
                        value_range=value_range,
                        gas_range=gas_range,
                        four_bytes_list=four_bytes_list,
                        full_transactions=full_transactions
                    )
                )
                
                # If we have enough transactions, stop sampling blocks
                if len(filtered_txs) >= sample_size:
                    break
            
            # If we still don't have enough transactions, sample more blocks
            while len(filtered_txs) < sample_size:
                # Sample a new block
                remaining_blocks = set(block_numbers) - set(sampled_blocks)
                if not remaining_blocks:
                    break
                    
                new_block = random.choice(list(remaining_blocks))
                sampled_blocks.append(new_block)
                
                filtered_txs.extend(
                    self._fetch_and_filter_block(
                        block_identifier=new_block,
                        to_addr_range=to_address_range,
                        value_range=value_range,
                        gas_range=gas_range,
                        four_bytes_list=four_bytes_list,
                        full_transactions=full_transactions
                    )
                )
        
        # If we have more transactions than needed, randomly sample
        if len(filtered_txs) > sample_size:
            filtered_txs = random.sample(filtered_txs, sample_size)
        # If we have fewer transactions than needed, raise an error
        elif len(filtered_txs) < sample_size:
            raise ValueError(f"Could not find {sample_size} transactions matching criteria. Only found {len(filtered_txs)}")
            
        return filtered_txs

    def _fetch_and_filter_block(
        self,
        block_identifier: Union[int, str],
        to_addr_range: Optional[Tuple[str, str]],
        value_range: Optional[Tuple[int, int]],
        gas_range: Optional[Tuple[int, int]],
        four_bytes_list: Optional[List[str]],
        full_transactions: int = 0
    ) -> Union[List[str], List[Dict]]:
        """
        Fetch and filter transactions from a block based on criteria.
        Returns either transaction hashes or transaction objects based on full_transactions parameter.

        Args:
            block_identifier: Block number or hash
            to_addr_range: Range of 'to' addresses to filter
            value_range: Range of transaction values to filter
            gas_range: Range of gas used to filter
            four_bytes_list: List of 4-byte function signatures to filter
            full_transactions: Level of transaction detail to return:
                0: Only transaction hashes
                1: Transaction data without logs
                2: Full transaction data with logs

        Returns:
            List of transaction hashes if full_transactions=0,
            otherwise list of transaction dictionaries
        """
        block = self.w3_client.eth.get_block(block_identifier, full_transactions=True)
        transactions = block.transactions

        result = []
        for tx in transactions:
            if self._match_filters(
                tx,
                to_addr_range=to_addr_range,
                value_range=value_range,
                gas_range=gas_range,
                four_bytes_list=four_bytes_list
            ):
                if full_transactions == 0:
                    # Return only transaction hash
                    result.append(tx.hash.hex())
                elif full_transactions == 1:
                    # Return transaction data without logs
                    tx_dict = dict(tx)
                    result.append(tx_dict)
                else:  # full_transactions == 2
                    # Return full transaction data with logs
                    tx_dict = dict(tx)
                    try:
                        tx_receipt = self.w3_client.eth.get_transaction_receipt(tx.hash)
                        tx_dict['receipt'] = dict(tx_receipt)
                    except Exception as e:
                        logger.warning(f"Failed to get receipt for tx {tx.hash.hex()}: {str(e)}")
                    result.append(tx_dict)

        return result

    def _match_filters(
        self,
        tx: Dict,
        to_addr_range: Optional[Tuple[str, str]],
        value_range: Optional[Tuple[int, int]],
        gas_range: Optional[Tuple[int, int]],
        four_bytes_list: Optional[List[str]],
    ) -> bool:
        """
        判断单笔交易 tx 是否满足所有过滤条件，满足则返回 True，否则 False。
        """
        # 1) to 地址过滤：基于字符串字典序比较
        if to_addr_range and tx["to"] is not None:
            start_to, end_to = to_addr_range
            to_lower = tx["to"].lower()
            if not (start_to.lower() <= to_lower <= end_to.lower()):
                return False

        # 2) value 范围过滤 (单位: Wei)
        if value_range:
            min_val, max_val = value_range
            if not (min_val <= tx["value"] <= max_val):
                return False

        # 3) gas 范围过滤
        if gas_range:
            min_gas, max_gas = gas_range
            if not (min_gas <= tx["gas"] <= max_gas):
                return False

        # 4) 4字节 (methodID) 过滤：交易 input 的前 4 字节
        if four_bytes_list:
            # 如果是简单转账，input 可能是 "0x"；做个保护
            tx_input = tx.get("input", "0x")
            if len(tx_input) < 10:  # 不含 methodID
                return False

            # 取 0x 后的前 8 个字符 => method_id
            method_id = tx_input[0:10].lower()  # 例如 "0xa9059cbb"
            # 对比 four_bytes_list 中是否有匹配
            if method_id not in [m.lower() for m in four_bytes_list]:
                return False

        # 全部条件都符合
        return True

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
        """Close all clients and cleanup resources"""
        for client in self._clients.values():
            if hasattr(client, 'close') and callable(client.close):
                if asyncio.iscoroutinefunction(client.close):
                    await client.close()
                else:
                    client.close()
            elif hasattr(client, 'session') and hasattr(client.session, 'close'):
                await client.session.close()
        
        # clients = [
        #     self.geckoterminal_client,
        #     self.gmgn_client,
        #     self.birdeye_client,
        #     self.solscan_client,
        #     self.goplus_client,
        #     self.dexscreener_client,
        #     self.chainbase_client,
        #     self.etherscan_client,
        #     self.opensearch_client,
        #     self.funding_client,
        #     self.label_client
        # ]
        
        # for client in clients:
        #     if client is not None and hasattr(client, 'close'):
        #         try:
        #             if asyncio.iscoroutinefunction(client.close):
        #                 await client.close()
        #             else:
        #                 client.close()
        #         except Exception as e:
        #             logger.warning(f"Error closing client {client.__class__.__name__}: {str(e)}")

    async def __aenter__(self):
        """Async context manager entry"""
        # Initialize clients that support async context managers
        clients = [
            self.geckoterminal_client,
            self.gmgn_client,
            self.birdeye_client,
            self.solscan_client,
            self.goplus_client,
            self.dexscreener_client,
            self.chainbase_client,
            self.etherscan_client,
            self.opensearch_client,
            self.funding_client,
            self.label_client
        ]
        
        for client in clients:
            if client is not None and hasattr(client, '__aenter__'):
                try:
                    await client.__aenter__()
                except Exception as e:
                    logger.warning(f"Error initializing client {client.__class__.__name__}: {str(e)}")
        
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit with proper cleanup"""
        await self.close()

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
                # Use loop.run_in_executor for blocking Web3 calls
                loop = asyncio.get_event_loop()
                
                # Get transactions and logs concurrently
                logs = await loop.run_in_executor(None, lambda: self.w3_client.eth.get_logs({
                    'fromBlock': block_number,
                    'toBlock': block_number,
                    'address': self.w3_client.toChecksumAddress(pair_address),
                    'topics': [
                        [UNI_V2_SWAP_TOPIC, UNI_V3_SWAP_TOPIC]
                    ]
                }))
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
                # Use loop.run_in_executor for blocking Web3 calls
                loop = asyncio.get_event_loop()
                logs = await loop.run_in_executor(None, lambda: self.w3_client.eth.get_logs({
                    'fromBlock': block_start,
                    'toBlock': block_end,
                    'address': self.w3_client.toChecksumAddress(pair_address),
                    'topics': [
                        [UNI_V2_SWAP_TOPIC, UNI_V3_SWAP_TOPIC]
                    ]
                }))
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
            # Use loop.run_in_executor for blocking Web3 calls
            loop = asyncio.get_event_loop()
            tx = await loop.run_in_executor(None, lambda: self.w3_client.eth.get_transaction(tx_hash))
            receipt = await loop.run_in_executor(None, lambda: self.w3_client.eth.get_transaction_receipt(tx_hash))

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
            # logger.info(f"getting tx by log: {log}")
            tx = await self.get_tx_with_logs_by_hash(log['transactionHash'])
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


    async def get_balance_changes(self, tx_hash: str) -> Dict[str, Dict[str, Any]]:
        """Get balance changes for ETH and tokens from transactions.
        
        Args:
            tx_hashes: List of transaction hashes
            
        Returns:
            Dict mapping address to their balance changes:
            {
                "address1": {
                    "eth_change": int,
                    "token_changes": {
                        "token_address": {
                            "amount": int,
                            "symbol": str,
                            "decimals": int
                        }
                    }
                }
            }
        """
        if not tx_hash:
            return {}

        return self.get_balance_changes_for_txs([tx_hash])

    async def get_balance_changes_for_txs(self, tx_hashes: List[str]) -> Dict[str, Dict[str, Any]]:
        """
        Get balance changes for multiple transactions.
        
        Args:
            tx_hashes: List of transaction hashes to analyze
            
        Returns:
            Dict mapping transaction hash to its balance changes
        """
        if not tx_hashes:
            return {}

        # Process transactions in batches of 100
        batch_size = 100
        
        for i in range(0, len(tx_hashes), batch_size):
            batch_hashes = tx_hashes[i:i + batch_size]
            logger.info(f"Processing batch {i//batch_size + 1} of {(len(tx_hashes) + batch_size - 1)//batch_size}")
            
            try:
                # Use rate-limited search from OpenSearch client
                results = await self.opensearch_client.search_transaction_batch(batch_hashes)
            except Exception as e:
                logger.error(f"Error searching transactions batch {i}-{i+batch_size}: {str(e)}")
                continue

            def _ensure_address(addr: str, changes: Dict):
                """Ensure address exists in changes dict"""
                if addr.lower() not in changes:
                    changes[addr.lower()] = {
                        'eth_change': 0,
                        'token_changes': {}
                    }

            # Process each transaction in the batch
            for hit in results["hits"]["hits"]:
                for tx_hit in hit["inner_hits"]["Transactions"]["hits"]["hits"]:
                    tx = tx_hit["_source"]
                    tx_hash = tx["Hash"]
                    tx_changes = {}
                    
                    # Skip failed transactions
                    if not tx.get("Status", False):
                        continue

                    # Process ETH balance changes from BalanceWrite
                    if "BalanceWrite" in tx:
                        for balance_write in tx["BalanceWrite"]:
                            address = balance_write["Address"].lower()
                            prev = int(balance_write["Prev"]) if balance_write["Prev"] != "0x" else 0
                            current = int(balance_write["Current"]) if balance_write["Current"] != "0x" else 0
                            
                            _ensure_address(address, tx_changes)
                            tx_changes[address]['eth_change'] = current - prev

                    # Process token balance changes from logs
                    if "Logs" in tx:
                        for log in tx["Logs"]:
                            # Skip non-Transfer events
                            if len(log.get("Topics", [])) != 3 or log["Topics"][0] != TRANSFER_EVENT_TOPIC:
                                continue
                                
                            # Get token contract and addresses
                            token_addr = log["Address"].lower()
                            from_addr = "0x" + log["Topics"][1][-40:].lower()
                            to_addr = "0x" + log["Topics"][2][-40:].lower()
                            
                            try:
                                amount = int(log["Data"], 16)
                            except ValueError:
                                logger.warning(f"Failed to parse token amount from log data: {log['Data']}")
                                continue

                            # Get token metadata
                            token_data = await self.get_token_metadata(token_addr)
                            
                            # From address loses tokens
                            _ensure_address(from_addr, tx_changes)
                            if token_addr not in tx_changes[from_addr]['token_changes']:
                                tx_changes[from_addr]['token_changes'][token_addr] = {
                                    'amount': 0,
                                    'symbol': token_data['symbol'] if token_data else '???',
                                    'decimals': token_data['decimals'] if token_data else 18
                                }
                            tx_changes[from_addr]['token_changes'][token_addr]['amount'] -= amount
                            
                            # To address gains tokens
                            _ensure_address(to_addr, tx_changes)
                            if token_addr not in tx_changes[to_addr]['token_changes']:
                                tx_changes[to_addr]['token_changes'][token_addr] = {
                                    'amount': 0,
                                    'symbol': token_data['symbol'] if token_data else '???',
                                    'decimals': token_data['decimals'] if token_data else 18
                                }
                            tx_changes[to_addr]['token_changes'][token_addr]['amount'] += amount
                    
                    all_changes[tx_hash] = tx_changes

        return all_changes

    async def aggregate_balance_changes(self, tx_hashes: List[str]) -> Dict[str, Dict[str, Any]]:
        """Aggregate balance changes across multiple transactions.
        
        Args:
            tx_hashes: List of transaction hashes to analyze
            
        Returns:
            Dict mapping addresses to their aggregated changes and related transactions:
            {
                "address": {
                    "eth_change": int,  # Total ETH change in wei
                    "token_changes": {
                        "token_address": {
                            "amount": int,  # Total token amount change
                            "symbol": str,  # Token symbol
                            "decimals": int # Token decimals
                        }
                    },
                    "transactions": [  # List of transactions affecting this address
                        {
                            "hash": str,  # Transaction hash
                            "eth_change": int,  # ETH change in this tx
                            "token_changes": {  # Token changes in this tx
                                "token_address": {
                                    "amount": int,
                                    "symbol": str,
                                    "decimals": int
                                }
                            }
                        }
                    ]
                }
            }
        """
        # Get individual transaction changes
        tx_changes = await self.get_balance_changes_for_txs(tx_hashes)
        
        # Initialize aggregated changes
        aggregated = {}
        
        # Process each transaction
        for tx_hash, changes in tx_changes.items():
            # Process each address in the transaction
            for address, addr_changes in changes.items():
                # Initialize address in aggregated if not exists
                if address not in aggregated:
                    aggregated[address] = {
                        'eth_change': 0,
                        'token_changes': {},
                        'transactions': []
                    }
                
                # Add ETH changes
                aggregated[address]['eth_change'] += addr_changes['eth_change']
                
                # Add token changes
                for token_addr, token_data in addr_changes.get('token_changes', {}).items():
                    if token_addr not in aggregated[address]['token_changes']:
                        aggregated[address]['token_changes'][token_addr] = {
                            'amount': 0,
                            'symbol': token_data['symbol'],
                            'decimals': token_data['decimals']
                        }
                    aggregated[address]['token_changes'][token_addr]['amount'] += token_data['amount']
                
                # Add transaction to address history
                aggregated[address]['transactions'].append({
                    'hash': tx_hash,
                    'eth_change': addr_changes['eth_change'],
                    'token_changes': addr_changes.get('token_changes', {})
                })
        
        return aggregated

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

    async def get_profit_ranking(self, tx_hashes: List[str]) -> List[Dict[str, Any]]:
        """Calculate profit ranking for addresses involved in transactions.
        Only considers ETH, WETH, USDC, USDT, and WBTC as profit sources.
        Ranks by positive profits only, but includes negative profits at the end.
        
        Token prices:
        - ETH/WETH: $3000
        - USDC/USDT: $1
        - WBTC: $100000
        
        Args:
            tx_hashes: List of transaction hashes to analyze
            
        Returns:
            List of dicts containing address and profit info, sorted by profit:
            [
                {
                    "address": str,
                    "total_profit_usd": float,
                    "profit_breakdown": {
                        "token_address": {
                            "symbol": str,
                            "amount": float,  # Token amount
                            "amount_usd": float  # USD value
                        }
                    },
                    "related_transactions": [  # List of transactions affecting this address
                        {
                            "hash": str,  # Transaction hash
                            "eth_change": float,  # ETH change in this tx
                            "token_changes": {  # Token changes in this tx
                                "token_address": {
                                    "amount": float,
                                    "symbol": str
                                }
                            }
                        }
                    ]
                }
            ]
        """
        # Token addresses and prices
        PROFIT_TOKENS = {
            "0x0000000000000000000000000000000000000000": {  # ETH
                "symbol": "ETH",
                "price": 3000,
                "decimals": 18
            },
            "0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2": {  # WETH
                "symbol": "WETH",
                "price": 3000,
                "decimals": 18
            },
            "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48": {  # USDC
                "symbol": "USDC",
                "price": 1,
                "decimals": 6
            },
            "0xdac17f958d2ee523a2206206994597c13d831ec7": {  # USDT
                "symbol": "USDT",
                "price": 1,
                "decimals": 6
            },
            "0x2260fac5e5542a773aa44fbcfedf7c193bc2c599": {  # WBTC
                "symbol": "WBTC",
                "price": 100000,
                "decimals": 8
            }
        }
        
        # Get aggregated changes
        aggregated = await self.aggregate_balance_changes(tx_hashes)
        
        # Calculate profits for each address
        profits = []
        losses = []  # Track losses separately
        for address, data in aggregated.items():
            profit_info = {
                "address": address,
                "total_profit_usd": 0,
                "profit_breakdown": {},
                "related_transactions": []
            }
            
            # Calculate ETH profit
            eth_change = data['eth_change'] / 1e18  # Convert wei to ETH
            if eth_change != 0:
                eth_profit_usd = eth_change * PROFIT_TOKENS["0x0000000000000000000000000000000000000000"]["price"]
                profit_info["profit_breakdown"]["0x0000000000000000000000000000000000000000"] = {
                    "symbol": "ETH",
                    "amount": eth_change,
                    "amount_usd": eth_profit_usd
                }
                profit_info["total_profit_usd"] += eth_profit_usd
            
            # Calculate token profits
            for token_addr, token_data in data.get('token_changes', {}).items():
                if token_addr in PROFIT_TOKENS:
                    token_info = PROFIT_TOKENS[token_addr]
                    amount = token_data['amount'] / (10 ** token_info['decimals'])
                    if amount != 0:
                        profit_usd = amount * token_info['price']
                        profit_info["profit_breakdown"][token_addr] = {
                            "symbol": token_info["symbol"],
                            "amount": amount,
                            "amount_usd": profit_usd
                        }
                        profit_info["total_profit_usd"] += profit_usd
            
            # Add related transactions with normalized values
            for tx in data.get('transactions', []):
                tx_info = {
                    "hash": tx['hash'],
                    "eth_change": tx['eth_change'] / 1e18,  # Convert wei to ETH
                    "token_changes": {}
                }
                
                # Normalize token amounts in transaction
                for token_addr, token_data in tx.get('token_changes', {}).items():
                    if token_addr in PROFIT_TOKENS:
                        token_info = PROFIT_TOKENS[token_addr]
                        amount = token_data['amount'] / (10 ** token_info['decimals'])
                        tx_info["token_changes"][token_addr] = {
                            "amount": amount,
                            "symbol": token_info["symbol"]
                        }
                
                profit_info["related_transactions"].append(tx_info)
            
            if profit_info["total_profit_usd"] > 0:  # Positive profit
                profits.append(profit_info)
            elif profit_info["total_profit_usd"] < 0:  # Negative profit (loss)
                losses.append(profit_info)
        
        # Sort profits by amount (descending) and losses by amount (ascending)
        profits.sort(key=lambda x: x["total_profit_usd"], reverse=True)
        losses.sort(key=lambda x: x["total_profit_usd"])
        
        # Return profits first, followed by losses
        return profits + losses

    async def get_token_transfer_txs(self, token_address: str) -> List[str]:
        """Get all distinct transactions containing transfers of a specific token.
        
        Args:
            token_address: The token contract address to look up
            
        Returns:
            List[str]: List of unique transaction hashes that contain transfers of this token
        """
        try:
            # Initialize PostgreSQL client
            self._get_client('postgres')
            
            # Convert to checksum address
            token_address = Web3.toChecksumAddress(token_address)
            
            # Query to get distinct transaction hashes
            query = """
                SELECT DISTINCT transaction_hash
                FROM token_transfers
                WHERE LOWER(token_address) = LOWER(%s)
                ORDER BY transaction_hash;
            """
            
            logger.info(f"Getting transactions for token: {token_address}")
            result = self.postgres_client.execute_query(query, [token_address])
            
            if result:
                tx_hashes = [row["transaction_hash"] for row in result]
                logger.info(f"Found {len(tx_hashes)} transactions for token {token_address}")
                return tx_hashes
            
            logger.warning(f"No transactions found for token: {token_address}")
            return []
            
        except Exception as e:
            logger.error(f"Error getting token transfer transactions: {str(e)}")
            return []

    async def get_token_deployer(self, token_address: str) -> Optional[str]:
        """Get the deployer address of a token contract.
        
        Args:
            token_address: Token contract address
            
        Returns:
            Optional[str]: Deployer address if found, None otherwise
        """
        try:
            # Get contract creation transaction
            creation_tx = await self.opensearch_client.get_contract_creation_tx(token_address)
            if creation_tx:
                return creation_tx.get('FromAddress')
            return None
        except Exception as e:
            logger.error(f"Error getting token deployer: {str(e)}")
            return None
            
    async def get_token_pairs(self, token_address: str) -> List[Dict[str, Any]]:
        """Get all trading pairs for a token.
        
        Args:
            token_address: Token contract address
            
        Returns:
            List[Dict[str, Any]]: List of pair info dictionaries
        """
        try:
            # Try getting pairs from various sources
            pairs = []
            
            # Try GeckoTerminal
            gt_pairs = await self.geckoterminal_client.get_token_pairs(token_address)
            if gt_pairs:
                pairs.extend(gt_pairs)
                
            # Try DexScreener
            ds_pairs = await self.dexscreener_client.get_token_pairs(token_address)
            if ds_pairs:
                pairs.extend(ds_pairs)
                
            return pairs
        except Exception as e:
            logger.error(f"Error getting token pairs: {str(e)}")
            return []
            
    async def check_token_trader(self, token_address: str, wallet_address: str) -> bool:
        """Check if an address has traded a specific token.
        
        Args:
            token_address: Token contract address
            wallet_address: Wallet address to check
            
        Returns:
            bool: True if the address has traded the token, False otherwise
        """
        try:
            # Query trading stats from OpenSearch
            trading_stats = await self.opensearch_client.get_token_trading_stats(
                token_address,
                wallet_address
            )
            return bool(trading_stats)
        except Exception as e:
            logger.error(f"Error checking token trader: {str(e)}")
            return False

    async def get_funder_address(self, address: str) -> Optional[str]:
        """
        Get the funder's address for a given address by:
        1. Getting the funding transaction hash from the funding service
        2. Querying the transaction details to get the 'from' address (funder)
        
        Args:
            address: The address to find the funder for
            
        Returns:
            Optional[str]: The funder's address if found, None otherwise
        """
        try:
            # Get funding transaction hash
            funding_result = await self.funding_client.simulate_view_first_fund(address)
            
            if not funding_result or 'result' not in funding_result:
                logger.error(f"No funding transaction found for address {address}")
                return None
            # print(funding_result['result'])
            tx_hash = funding_result['result']['TxnHash']
            if not tx_hash:
                logger.error(f"Empty transaction hash returned for address {address}")
                return None
            
            # Get transaction details using web3
            tx = self.w3_client.eth.get_transaction(tx_hash)
            if not tx:
                logger.error(f"Could not find transaction {tx_hash}")
                return None
                
            return tx['from']
            
        except Exception as e:
            logger.error(f"Error getting funder address for {address}: {str(e)}")
            return None

    async def get_funder_addresses(self, addresses: List[str]) -> Dict[str, Optional[str]]:
        """
        Get the funder's addresses for a list of addresses by:
        1. Getting the funding transaction hashes in batch from the funding service
        2. Querying the transaction details to get the 'from' addresses (funders)
        
        Args:
            addresses: List of addresses to find the funders for
            
        Returns:
            Dict[str, Optional[str]]: Dictionary mapping each input address to its funder's address
        """
        try:
            # Get funding transaction hashes in batch
            funding_results = await self.funding_client.batch_simulate_view_first_fund(addresses)
            
            if not funding_results:
                logger.error("No funding transactions found")
                return {addr: None for addr in addresses}

            # Process results and get transaction details
            funder_map = {}
            for addr, result in zip(addresses, funding_results):
                try:
                    if not result or 'result' not in result or not result['result']:
                        logger.error(f"No funding transaction found for address {addr}")
                        funder_map[addr] = None
                        continue

                    result_data = result['result']
                    tx_hash = result_data.get('TxnHash')
                    if not tx_hash:
                        logger.error(f"Empty transaction hash returned for address {addr}")
                        funder_map[addr] = None
                        continue

                    # Try to get funder from API response first
                    next_funder = result_data.get('From')
                    
                    if not next_funder:
                        # Fallback to transaction lookup
                        try:
                            loop = asyncio.get_event_loop()
                            tx = await loop.run_in_executor(
                                None,
                                self.w3_client.eth.get_transaction,
                                tx_hash
                            )
                            if not tx:
                                logger.error(f"No transaction found for hash {tx_hash}")
                                funder_map[addr] = None
                                continue
                            next_funder = tx['from']
                        except Exception as e:
                            logger.error(f"Error getting transaction {tx_hash}: {str(e)}")
                            funder_map[addr] = None
                            continue

                    funder_map[addr] = next_funder

                except Exception as e:
                    logger.error(f"Error processing address {addr}: {str(e)}")
                    funder_map[addr] = None

            return funder_map
            
        except Exception as e:
            logger.error(f"Error getting funder addresses: {str(e)}")
            return {addr: None for addr in addresses}

    async def get_funder_tree(
        self, 
        addresses: List[str], 
        max_depth: int = 3,
        stop_at_cex: bool = True
    ) -> Dict[str, Any]:
        """
        Recursively get the funder tree for given addresses up to a specified depth.
        
        Args:
            addresses: List of addresses to find the funders for
            max_depth: Maximum depth to traverse up the funding chain (default: 3)
            stop_at_cex: If True, stops traversing when a CEX/EXCHANGE is found (default: True)
            
        Returns:
            Dict[str, Any]: Nested dictionary representing the funding tree where each level contains:
                - funder: The funder's address
                - funded_at: Transaction hash of the funding
                - is_cex: Boolean indicating if the funder is a CEX/EXCHANGE
                - next_level: Recursive funding information for the funder (if within max_depth and not stopped at CEX)
        """
        if max_depth <= 0 or not addresses:
            return {}

        try:
            # Process addresses in batches of 10 for efficiency
            tree = {}
            batch_size = 10
            
            for i in range(0, len(addresses), batch_size):
                batch_addresses = addresses[i:i + batch_size]
                
                # Get funding information for current batch
                funding_results = await self.funding_client.batch_simulate_view_first_fund(batch_addresses)
                
                if not funding_results:
                    logger.error(f"No funding transactions found for addresses: {batch_addresses}")
                    tree.update({addr: None for addr in batch_addresses})
                    continue

                # Process results and build tree
                next_level_addresses = set()  # Using set to avoid duplicate funder addresses
                funders_to_check = set()  # Collect funders to check labels in batch

                # First pass: build current level and collect funders to check
                for addr, result in zip(batch_addresses, funding_results):
                    try:
                        if not result or 'result' not in result or not result['result']:
                            logger.error(f"No funding transaction found for address {addr}")
                            tree[addr] = None
                            continue

                        result_data = result['result']
                        tx_hash = result_data.get('TxnHash')
                        if not tx_hash:
                            logger.error(f"Empty transaction hash returned for address {addr}")
                            tree[addr] = None
                            continue

                        # Get funder address
                        next_funder = result_data.get('From')
                        if not next_funder:
                            try:
                                tx = await asyncio.get_event_loop().run_in_executor(
                                    None,
                                    self.w3_client.eth.get_transaction,
                                    tx_hash
                                )
                                if not tx:
                                    logger.error(f"No transaction found for hash {tx_hash}")
                                    tree[addr] = None
                                    continue
                                next_funder = tx['from']
                            except Exception as e:
                                logger.error(f"Error getting transaction {tx_hash}: {str(e)}")
                                tree[addr] = None
                                continue

                        tree[addr] = {
                            "funder": next_funder,
                            "funded_at": tx_hash,
                            "is_cex": False,  # Will be updated in batch
                            "next_level": {}  # Initialize as empty dict
                        }
                        funders_to_check.add(next_funder)
                        next_level_addresses.add(next_funder)

                    except Exception as e:
                        logger.error(f"Error processing address {addr}: {str(e)}")
                        tree[addr] = None

                # Check labels for all funders in this batch
                if funders_to_check:
                    funder_labels = self._get_client('label').get_addresses_labels(list(funders_to_check))
                    funder_is_cex = {
                        label_info['address']: label_info['is_cex']
                        for label_info in funder_labels
                    }
                    
                    # Update tree with CEX information
                    for addr in batch_addresses:
                        if tree.get(addr) and tree[addr].get('funder'):
                            funder = tree[addr]['funder']
                            is_cex = funder_is_cex.get(funder, False)
                            name_tag = funder_is_cex.get(funder, '').upper()
                            is_cex = any(cex_term in name_tag for cex_term in ['BINANCE', 'HUOBI', 'COINBASE', 'KRAKEN', 'BITFINEX', 'EXCHANGE'])
                            tree[addr]['is_cex'] = is_cex
                            
                            # If this is a CEX and we should stop, don't traverse further
                            if stop_at_cex and is_cex:
                                tree[addr]['next_level'] = None
                            else:
                                next_level_addresses.add(funder)

                # Recursively get next level for addresses we're continuing to traverse
                if next_level_addresses and max_depth > 1:
                    next_level = await self.get_funder_tree(
                        list(next_level_addresses),
                        max_depth - 1,
                        stop_at_cex
                    )
                    
                    # Update tree with next level results
                    for addr in batch_addresses:
                        if (tree.get(addr) and tree[addr].get('funder') and 
                            tree[addr]['next_level'] is not None):
                            funder = tree[addr]['funder']
                            tree[addr]['next_level'] = next_level.get(funder)

            return tree
            
        except Exception as e:
            logger.error(f"Error in get_funder_tree: {str(e)}")
            return {addr: None for addr in addresses}

    async def get_root_funder(self, address: str, max_depth: int = 100) -> Optional[Dict[str, Any]]:
        """
        Get the root funder (the earliest funder with no further funding source) for a given address.
        This function will keep searching deeper until it finds an address with no funding source,
        or until it reaches max_depth.
        
        Args:
            address: The address to find the root funder for
            max_depth: Maximum depth to prevent infinite loops (default: 20)

        Returns:
            Optional[Dict[str, Any]]: Dictionary containing:
                - address: The root funder's address
                - tx_hash: The transaction hash that funded the previous address
                - depth: How many levels deep we found this funder
                - is_root: True if this is confirmed to be the root funder (no further funding found)
            Returns None if no funding information is found
        """
        try:
            current_address = address
            current_depth = 0
            last_tx_hash = None
            
            while current_depth < max_depth:
                # Try to get next funder
                result = await self.funding_client.simulate_view_first_fund(current_address)
                
                # If no funding info found, we've reached a root funder
                if not result or 'result' not in result or not result['result']:
                    if current_depth > 0:
                        return {
                            "address": current_address,
                            "tx_hash": last_tx_hash,
                            "depth": current_depth,
                            "is_root": True  # Confirmed root as no further funding found
                        }
                    return None

                result_data = result['result']
                tx_hash = result_data.get('TxnHash')
                if not tx_hash:
                    if current_depth > 0:
                        return {
                            "address": current_address,
                            "tx_hash": last_tx_hash,
                            "depth": current_depth,
                            "is_root": True  # No transaction hash means no further funding
                        }
                    return None

                # Try to get funder from API response first
                next_funder = result_data.get('From')
                
                if not next_funder:
                    # Fallback to transaction lookup
                    try:
                        loop = asyncio.get_event_loop()
                        tx = await loop.run_in_executor(None, lambda: self.w3_client.eth.get_transaction(tx_hash))
                        if not tx:
                            if current_depth > 0:
                                return {
                                    "address": current_address,
                                    "tx_hash": last_tx_hash,
                                    "depth": current_depth,
                                    "is_root": True  # No transaction means no further funding
                                }
                            return None
                        next_funder = tx['from']
                    except Exception as tx_error:
                        logger.error(f"Error getting transaction {tx_hash}: {str(tx_error)}")
                        if current_depth > 0:
                            return {
                                "address": current_address,
                                "tx_hash": last_tx_hash,
                                "depth": current_depth,
                                "is_root": False  # Error means we're not sure if this is root
                            }
                        return None

                # Update for next iteration
                last_tx_hash = tx_hash
                current_address = next_funder
                current_depth += 1

            # If we reach max depth, return the last funder but indicate it might not be root
            return {
                "address": current_address,
                "tx_hash": last_tx_hash,
                "depth": current_depth,
                "is_root": False  # Reached max depth, might not be actual root
            }

        except Exception as e:
            logger.error(f"Error getting root funder for {address}: {str(e)}")
            return None

    async def get_root_funders(self, addresses: List[str], max_depth: int = 100) -> Dict[str, Optional[Dict[str, Any]]]:
        """
        Get the root funders (earliest funders with no further funding sources) for a list of addresses.
        
        Args:
            addresses: List of addresses to find the root funders for
            max_depth: Maximum depth to prevent infinite loops (default: 20)
            
        Returns:
            Dict[str, Optional[Dict[str, Any]]]: Dictionary mapping input addresses to their root funder info.
            Each root funder info contains:
                - address: The root funder's address
                - tx_hash: The transaction hash that funded the previous address
                - depth: How many levels deep we found this funder
                - is_root: True if this is confirmed to be the root funder
        """
        tasks = [self.get_root_funder(addr, max_depth) for addr in addresses]
        results = await asyncio.gather(*tasks)
        return dict(zip(addresses, results))

    @file_cache(namespace="funding_path", ttl=3600*24)  # Cache for 24 hours
    async def get_funding_path(self, address: str, max_depth: int = 20, stop_at_cex: bool = True) -> List[Dict[str, Any]]:
        """
        Get the complete funding path for an address up to the root funder or first CEX.
        
        Args:
            address: The address to get the funding path for
            max_depth: Maximum depth to search (default: 20)
            stop_at_cex: If True, stops at first CEX found (default: True)
            
        Returns:
            List[Dict[str, Any]]: List of funding steps, each containing:
                - address: The funder's address
                - tx_hash: The transaction hash
                - depth: The depth level of this funder
                - is_cex: Boolean indicating if the funder is a CEX/EXCHANGE
                - label: The address label (or "Default" if none)
                - name_tag: The name tag of the address (if any)
                - type: The type of the address (if any)
                - entity: The entity type of the address (if any)
        """
        path = []
        current_address = address
        depth = 0
        BATCH_SIZE = 10
        pending_funders = []
        pending_txs = []
        
        try:
            while depth < max_depth:
                # Get funding info for current address
                result = await self.funding_client.simulate_view_first_fund(current_address)
                
                # If no funding info found, we've reached the end
                if not result or 'result' not in result or not result['result']:
                    break
                    
                result_data = result['result']
                tx_hash = result_data.get('TxnHash')
                if not tx_hash:
                    break
                    
                # Get funder address
                next_funder = result_data.get('From')
                if not next_funder:
                    try:
                        tx = await asyncio.get_event_loop().run_in_executor(
                            None,
                            self.w3_client.eth.get_transaction,
                            tx_hash
                        )
                        if not tx:
                            break
                        next_funder = tx['from']
                    except Exception as e:
                        logger.error(f"Error getting transaction {tx_hash}: {str(e)}")
                        break
                
                # Add to pending lists
                pending_funders.append(next_funder)
                pending_txs.append((tx_hash, depth + 1))
                
                # Process batch if we've reached BATCH_SIZE or at max depth
                if len(pending_funders) >= BATCH_SIZE or depth == max_depth - 1:
                    # Get labels for the batch
                    if self.label_client:
                        try:
                            label_results = self._get_client('label').get_addresses_labels(pending_funders)
                            label_map = {
                                result['address'].lower(): result 
                                for result in label_results
                            }
                            
                            # Process each pending funder
                            for i, funder in enumerate(pending_funders):
                                funder_lower = funder.lower()
                                tx_hash, cur_depth = pending_txs[i]
                                label_info = label_map.get(funder_lower, {})
                                
                                # Check if it's a CEX
                                label_type = (label_info.get('type') or 'DEFAULT').upper()
                                name_tag = (label_info.get('name_tag') or '').upper()
                                is_cex = any(cex_term in label_type for cex_term in ['CEX', 'EXCHANGE'])
                                # Add to path
                                path.append({
                                    'address': funder,
                                    'tx_hash': tx_hash,
                                    'depth': cur_depth,
                                    'is_cex': is_cex,
                                    'label': label_info.get('label', 'Default'),
                                    'name_tag': name_tag,
                                    'type': label_type,
                                    'entity': label_info.get('entity')
                                })
                                
                                # If this is a CEX and we should stop, prune the path and return
                                if stop_at_cex and is_cex:
                                    return path[:path.index(path[-1]) + 1]
                            
                        except Exception as e:
                            logger.error(f"Error getting labels for funders batch: {str(e)}")
                            # Add funders without labels
                            for i, funder in enumerate(pending_funders):
                                tx_hash, cur_depth = pending_txs[i]
                                path.append({
                                    'address': funder,
                                    'tx_hash': tx_hash,
                                    'depth': cur_depth,
                                    'is_cex': False,
                                    'label': 'Default',
                                    'name_tag': '',
                                    'type': '',
                                    'entity': ''
                                })
                    
                    # Clear pending lists
                    pending_funders = []
                    pending_txs = []
                
                current_address = next_funder
                depth += 1
            
            # Process any remaining funders
            if pending_funders:
                if self.label_client:
                    try:
                        label_results = self._get_client('label').get_addresses_labels(pending_funders)
                        label_map = {
                            result['address'].lower(): result 
                            for result in label_results
                        }
                        
                        for i, funder in enumerate(pending_funders):
                            funder_lower = funder.lower()
                            tx_hash, cur_depth = pending_txs[i]
                            label_info = label_map.get(funder_lower, {})
                            
                            label_type = (label_info.get('type') or 'DEFAULT').upper()
                            name_tag = (label_info.get('name_tag') or '').upper()
                            is_cex = any(cex_term in label_type for cex_term in ['CEX', 'EXCHANGE'])
                            entity = label_info.get('entity')
                            
                            path.append({
                                'address': funder,
                                'tx_hash': tx_hash,
                                'depth': cur_depth,
                                'is_cex': is_cex,
                                'label': label_info.get('label', 'Default'),
                                'name_tag': name_tag,
                                'type': label_type,
                                'entity': entity
                            })
                            
                            if stop_at_cex and is_cex:
                                return path[:path.index(path[-1]) + 1]
                            
                    except Exception as e:
                        logger.error(f"Error getting labels for remaining funders: {str(e)}")
                        for i, funder in enumerate(pending_funders):
                            tx_hash, cur_depth = pending_txs[i]
                            path.append({
                                'address': funder,
                                'tx_hash': tx_hash,
                                'depth': cur_depth,
                                'is_cex': False,
                                'label': 'Default',
                                'name_tag': '',
                                'type': '',
                                'entity': ''
                            })
            
            return path
            
        except Exception as e:
            logger.error(f"Error getting funding path: {str(e)}")
            return path

    @file_cache(namespace="funding_relationship", ttl=3600*24)  # Cache for 24 hours
    async def check_funding_relationship(
        self, 
        address1: str, 
        address2: str, 
        max_depth: int = 20,
        stop_at_cex: bool = True
    ) -> Optional[Dict[str, Any]]:
        """
        Check if two addresses have a funding relationship by finding common funders
        in their funding paths. A common funder can be:
        1. Same address in both paths
        2. Different addresses but belong to the same non-empty entity
        
        Args:
            address1: First address to check
            address2: Second address to check
            max_depth: Maximum depth to search in each path (default: 20)
            stop_at_cex: If True, stops at first CEX found (default: True)
            
        Returns:
            Optional[Dict[str, Any]]: If a relationship is found, returns:
                - common_funder1: The funder's address in first path
                - common_funder2: The funder's address in second path (same as common_funder1 if same address)
                - depth1: Depth of funder in first address's path
                - depth2: Depth of funder in second address's path
                - tx_hash1: Transaction hash from funder to first path
                - tx_hash2: Transaction hash from funder to second path
                - common_type: Type of relationship (1: same entity, 2: same address with empty entity, 0: no relationship)
                - label: The funder's label
                - name_tag: The name tag of the funder
                - type: The type of the funder
                - entity: The entity of the funder (if any)
            Returns None if no relationship is found
        """
        try:
            # Get funding paths for both addresses
            path1 = await self.get_funding_path(address1, max_depth, stop_at_cex)
            path2 = await self.get_funding_path(address2, max_depth, stop_at_cex)
            # Create maps for faster lookup
            path1_map = {step['address']: step for step in path1}
            path2_map = {step['address']: step for step in path2}
            
            # Find relationships
            relationships = []
            
            # Case 1: Same address funders
            common_addresses = set(path1_map.keys()) & set(path2_map.keys())
            for addr in common_addresses:
                funder1 = path1_map[addr]
                funder2 = path2_map[addr]
                common_type = 1 if funder1.get('entity') and funder1['entity'] == funder2.get('entity') else 2
                relationships.append({
                    'common_funder1': addr,
                    'common_funder2': addr,
                    'funder1': funder1,
                    'funder2': funder2,
                    'combined_depth': funder1['depth'] + funder2['depth'],
                    'common_type': common_type
                })
            
            # Case 2: Different addresses with same non-empty entity
            entity_map1 = {}
            entity_map2 = {}
            
            for addr, data in path1_map.items():
                entity = data.get('entity')
                if entity:
                    if entity not in entity_map1:
                        entity_map1[entity] = []
                    entity_map1[entity].append((addr, data))
                    
            for addr, data in path2_map.items():
                entity = data.get('entity')
                if entity:
                    if entity not in entity_map2:
                        entity_map2[entity] = []
                    entity_map2[entity].append((addr, data))
            
            # Find addresses with same entity
            common_entities = set(entity_map1.keys()) & set(entity_map2.keys())
            for entity in common_entities:
                for addr1, funder1 in entity_map1[entity]:
                    for addr2, funder2 in entity_map2[entity]:
                        if addr1 != addr2:  # Skip if same address (already handled in Case 1)
                            relationships.append({
                                'common_funder1': addr1,
                                'common_funder2': addr2,
                                'funder1': funder1,
                                'funder2': funder2,
                                'combined_depth': funder1['depth'] + funder2['depth'],
                                'common_type': 1  # Same entity
                            })
            
            if not relationships:
                return None
                
            # Find the closest relationship (minimum combined depth)
            closest = min(relationships, key=lambda x: x['combined_depth'])
            
            return {
                'common_funder1': closest['common_funder1'],
                'common_funder2': closest['common_funder2'],
                'depth1': closest['funder1']['depth'],
                'depth2': closest['funder2']['depth'],
                'tx_hash1': closest['funder1']['tx_hash'],
                'tx_hash2': closest['funder2']['tx_hash'],
                'common_type': closest['common_type'],
                'label': closest['funder1']['label'],
                'name_tag': closest['funder1'].get('name_tag', ''),
                'type': closest['funder1'].get('type', 'EOA'),
                'entity': closest['funder1'].get('entity', '')
            }
            
        except Exception as e:
            logger.error(f"Error checking funding relationship: {str(e)}")
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

    async def get_token_metadata(self, token_address: str) -> Optional[Dict[str, Any]]:
        """Get token metadata from the database.
        
        Args:
            token_address: Token contract address
            
        Returns:
            Optional[Dict[str, Any]]: Token metadata if found, None otherwise
        """
        try:
            client = self.postgres_client
            query = """
                SELECT symbol, decimals
                FROM eth_tokens 
                WHERE address = %s
            """
            result = client.execute_query(query, [token_address.lower()])
            
            if result and len(result) > 0:
                return {
                    'symbol': result[0]['symbol'],
                    'decimals': result[0]['decimals']
                }
            return None
        except Exception as e:
            logger.error(f"Error getting token metadata: {str(e)}")
            return None