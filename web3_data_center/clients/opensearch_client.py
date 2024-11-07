from typing import List, Dict, Any, Optional
import asyncio
import logging
from urllib.parse import urlparse
import traceback

from opensearchpy import AsyncOpenSearch, OpenSearch,ConnectionTimeout, OpenSearchException, NotFoundError
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type

from opensearchpy import OpenSearch, RequestError, TransportError

from .base_client import BaseClient

logger = logging.getLogger(__name__)

class OpenSearchClient(BaseClient):
    def __init__(self, config_path: str = "config.yml", use_proxy: bool = False):
        super().__init__('opensearch', config_path=config_path, use_proxy=use_proxy)
        
        parsed_url = urlparse(self.config['api']['opensearch']['hosts'][0])
        self.client = AsyncOpenSearch(
            hosts=[{'host': parsed_url.hostname, 'port': parsed_url.port or 443}],
            http_auth=(self.credentials['username'], self.credentials['password']),
            use_ssl=parsed_url.scheme == 'https',
            verify_certs=True,
            timeout=self.config['api']['opensearch'].get('timeout', 120)
        )
        # logger.info(f"OpenSearch host: {self.config['api']['opensearch']['hosts']}")
        # logger.info(f"OpenSearch username: {self.credentials['username']}")
        # logger.info(f"OpenSearch password: {self.credentials['password']}")

    async def test_connection(self) -> bool:
        try:
            info = await self.client.info()
            # logger.info(f"Successfully connected to OpenSearch cluster: {info['cluster_name']}")
            # logger.info(f"OpenSearch version: {info['version']['number']}")
            return True
        except Exception as e:
            logger.error(f"Failed to connect to OpenSearch: {e}")
            return False

    async def check_index_exists(self, index: str) -> bool:
        try:
            exists = await self.client.indices.exists(index=index)
            if exists:
                logger.info(f"Index '{index}' exists")
            else:
                logger.warning(f"Index '{index}' does not exist")
            return exists
        except Exception as e:
            logger.error(f"Error checking index existence: {e}")
            return False

    @retry(
        stop=stop_after_attempt(5),
        wait=wait_exponential(multiplier=1, min=4, max=10),
        retry=retry_if_exception_type((ConnectionTimeout, OpenSearchException)),
        reraise=True
    )
    async def search_logs(self, index: str, start_block: int, end_block: int, 
                          event_topics: List[str], size: int = 1000) -> List[Dict[str, Any]]:
        query = self._build_query(start_block, end_block, event_topics, size)
        
        try:
            response = await self.client.search(index=index, body=query, scroll='2m')
            scroll_id = response['_scroll_id']
            hits = response['hits']['hits']

            while len(response['hits']['hits']) > 0:
                response = await self.client.scroll(scroll_id=scroll_id, scroll='2m')
                scroll_id = response['_scroll_id']
                hits.extend(response['hits']['hits'])

            return hits
        except ConnectionTimeout as e:
            logger.error(f"Connection timeout occurred: {e}. Retrying...")
            raise
        except OpenSearchException as e:
            logger.error(f"OpenSearch exception occurred: {e}")
            raise
        finally:
            if 'scroll_id' in locals():
                await self.client.clear_scroll(scroll_id=scroll_id)

    @staticmethod
    def _build_query(start_block: int, end_block: int, event_topics: List[str], size: int) -> Dict[str, Any]:
        return {
            "query": {
                "bool": {
                    "must": [
                        {"range": {"Number": {"gte": start_block, "lte": end_block}}},
                        {"nested": {
                            "path": "Transactions.Logs",
                            "query": {
                                "terms": {
                                    "Transactions.Logs.Topics": event_topics
                                }
                            }
                        }}
                    ]
                }
            },
            "size": size,
            "_source": ["Number", "Transactions.Hash", "Transactions.FromAddress", "Transactions.ToAddress", "Transactions.Logs.Topics", "Transactions.Logs.Data", "Transactions.Logs.Address"],
            "sort": [{"Number": {"order": "asc"}}]
        }

    async def close(self):
        await self.client.close()
    @staticmethod
    def _build_specific_txs_query(to_address: str, start_block: int, end_block: int, size: int) -> Dict[str, Any]:
                return {
        "query": {
            "bool": {
                "must": [
                    {
                        "range": {
                            "Number": {
                                "gte": start_block,
                                "lte": end_block
                            }
                        }
                    },
                    {
                        "nested": {
                            "path": "Transactions",
                            "query": {
                                "term": {
                                    "Transactions.ToAddress": to_address
                                }
                            },
                            "inner_hits": {
                                "size": 2000,
                                "_source": True
                            }
                        }
                    }
                ]
            }
        },
        "size": size,
        "_source": ["Number", "Timestamp"],
        "sort": [
            {
                "Number": {
                    "order": "asc"
                }
            }
        ]
    }
    

 
    async def get_specific_txs(self, to_address: str, start_block: int, end_block: int, size: int = 1000, max_iterations: int = 1000000000) -> List[Dict[str, Any]]:
        query = self._build_specific_txs_query(to_address, start_block, end_block, size)

        transactions = []
        iteration_count = 0
        total_hits = 0

        try:
            response = await self.client.search(index="eth_block", body=query, scroll='2m')
            scroll_id = response['_scroll_id']
            while response['hits']['hits']:
                for hit in response['hits']['hits']:
                    block_number = hit['_source']['Number']
                    timestamp = hit['_source']['Timestamp']
                    for tx in hit['inner_hits']['Transactions']['hits']['hits']:
                        tx_source = tx['_source']
                        if tx_source.get('ToAddress') == to_address:
                            processed_tx = {
                                'block_number': block_number,
                                'timestamp': timestamp,
                                'hash': tx_source.get('Hash'),
                                'from_address': tx_source.get('FromAddress'),
                                'to_address': tx_source.get('ToAddress'),
                                'value': tx_source.get('Value'),
                                'gas_price': tx_source.get('GasPrice'),
                                'gas_limit': tx_source.get('GasLimit'),
                                'gas_used': tx_source.get('GasUsed'),
                                'gas_used_exec': tx_source.get('GasUsedExec'),
                                'gas_used_init': tx_source.get('GasUsedInit'),
                                'gas_used_refund': tx_source.get('GasUsedRefund'),
                                'nonce': tx_source.get('Nonce'),
                                'status': tx_source.get('Status'),
                                'type': tx_source.get('Type'),
                                'txn_index': tx_source.get('TxnIndex'),
                                'call_function': tx_source.get('CallFunction'),
                                'call_parameter': tx_source.get('CallParameter'),
                                'gas_fee_cap': tx_source.get('GasFeeCap'),
                                'gas_tip_cap': tx_source.get('GasTipCap'),
                                'blob_fee_cap': tx_source.get('BlobFeeCap'),
                                'blob_hashes': tx_source.get('BlobHashes'),
                                'con_address': tx_source.get('ConAddress'),
                                'cum_gas_used': tx_source.get('CumGasUsed'),
                                'error_info': tx_source.get('ErrorInfo'),
                                'int_txn_count': tx_source.get('IntTxnCount'),
                                'output': tx_source.get('Output'),
                                'serial_number': tx_source.get('SerialNumber'),
                                'access_list': tx_source.get('AccessList'),
                                'balance_read': tx_source.get('BalanceRead'),
                                'balance_write': tx_source.get('BalanceWrite'),
                                'code_info_read': tx_source.get('CodeInfoRead'),
                                'code_read': tx_source.get('CodeRead'),
                                'code_write': tx_source.get('CodeWrite'),
                                'created': tx_source.get('Created'),
                                'internal_txns': tx_source.get('InternalTxns'),
                                'logs': tx_source.get('Logs'),
                                'nonce_read': tx_source.get('NonceRead'),
                                'nonce_write': tx_source.get('NonceWrite'),
                                'storage_read': tx_source.get('StorageRead'),
                                'storage_write': tx_source.get('StorageWrite'),
                                'suicided': tx_source.get('Suicided')
                            }
                            transactions.append(processed_tx)
                total_hits += len(response['hits']['hits'])
                iteration_count += 1
                if iteration_count >= max_iterations:
                    logger.warning(f"Reached maximum number of iterations ({max_iterations}) in get_specific_txs")
                    break
                response = await self.client.scroll(scroll_id=scroll_id, scroll='2m')
                scroll_id = response['_scroll_id']
            
            await self.client.clear_scroll(scroll_id=scroll_id)
            logger.info(f"Processed {total_hits} hits, retrieved {len(transactions)} matching transactions")
            return transactions
        except RequestError as e:
            logger.error(f"OpenSearch request error: {e}")
            logger.error(f"Query: {query}")
            logger.error(f"Error details: {e.info}")
            raise
        except TransportError as e:
            logger.error(f"OpenSearch transport error: {e}")
            logger.error(f"Query: {query}")
            raise
        except Exception as e:
            logger.error(f"Unexpected error in get_specific_txs: {e}")
            logger.error(f"Query: {query}")
            logger.error(f"Traceback: {traceback.format_exc()}")
            raise

    async def get_specific_txs_batched(self, to_address: str, start_block: int, end_block: int, size: int = 1000, max_iterations: int = 1000000000) -> List[Dict[str, Any]]:
        query = self._build_specific_txs_query(to_address, start_block, end_block, size)

        iteration_count = 0
        total_hits = 0

        # search_after = None
        try:
            response = await self.client.search(index="eth_block", body=query, scroll='2m')
            scroll_id = response['_scroll_id']
            while response['hits']['hits']:
                batch_transactions = []
                for hit in response['hits']['hits']:
                    block_number = hit['_source']['Number']
                    timestamp = hit['_source']['Timestamp']
                    for tx in hit['inner_hits']['Transactions']['hits']['hits']:
                        tx_source = tx['_source']
                        if tx_source.get('ToAddress') == to_address:
                            processed_tx = {
                                'block_number': block_number,
                                'timestamp': timestamp,
                                'hash': tx_source.get('Hash'),
                                'from_address': tx_source.get('FromAddress'),
                                'to_address': tx_source.get('ToAddress'),
                                'value': tx_source.get('Value'),
                                'gas_price': tx_source.get('GasPrice'),
                                'gas_limit': tx_source.get('GasLimit'),
                                'gas_used': tx_source.get('GasUsed'),
                                'gas_used_exec': tx_source.get('GasUsedExec'),
                                'gas_used_init': tx_source.get('GasUsedInit'),
                                'gas_used_refund': tx_source.get('GasUsedRefund'),
                                'nonce': tx_source.get('Nonce'),
                                'status': tx_source.get('Status'),
                                'type': tx_source.get('Type'),
                                'txn_index': tx_source.get('TxnIndex'),
                                'call_function': tx_source.get('CallFunction'),
                                'call_parameter': tx_source.get('CallParameter'),
                                'gas_fee_cap': tx_source.get('GasFeeCap'),
                                'gas_tip_cap': tx_source.get('GasTipCap'),
                                'blob_fee_cap': tx_source.get('BlobFeeCap'),
                                'blob_hashes': tx_source.get('BlobHashes'),
                                'con_address': tx_source.get('ConAddress'),
                                'cum_gas_used': tx_source.get('CumGasUsed'),
                                'error_info': tx_source.get('ErrorInfo'),
                                'int_txn_count': tx_source.get('IntTxnCount'),
                                'output': tx_source.get('Output'),
                                'serial_number': tx_source.get('SerialNumber'),
                                'access_list': tx_source.get('AccessList'),
                                'balance_read': tx_source.get('BalanceRead'),
                                'balance_write': tx_source.get('BalanceWrite'),
                                'code_info_read': tx_source.get('CodeInfoRead'),
                                'code_read': tx_source.get('CodeRead'),
                                'code_write': tx_source.get('CodeWrite'),
                                'created': tx_source.get('Created'),
                                'internal_txns': tx_source.get('InternalTxns'),
                                'logs': tx_source.get('Logs'),
                                'nonce_read': tx_source.get('NonceRead'),
                                'nonce_write': tx_source.get('NonceWrite'),
                                'storage_read': tx_source.get('StorageRead'),
                                'storage_write': tx_source.get('StorageWrite'),
                                'suicided': tx_source.get('Suicided')
                            }
                            batch_transactions.append(processed_tx)
                total_hits += len(response['hits']['hits'])
                iteration_count += 1
                yield batch_transactions

                if iteration_count >= max_iterations:
                    logger.warning(f"Reached maximum number of iterations ({max_iterations}) in get_specific_txs_batch")
                    break
                response = await self.client.scroll(scroll_id=scroll_id, scroll='2m')
                scroll_id = response['_scroll_id']

            await self.client.clear_scroll(scroll_id=scroll_id)
            logger.info(f"Processed {total_hits} hits in {iteration_count} iterations")
        except RequestError as e:
            logger.error(f"OpenSearch request error: {e}")
            logger.error(f"Query: {query}")
            logger.error(f"Error details: {e.info}")
            raise
        except TransportError as e:
            logger.error(f"OpenSearch transport error: {e}")
            logger.error(f"Query: {query}")
            raise
        except Exception as e:
            logger.error(f"Unexpected error in get_specific_txs_batch: {e}")
            logger.error(f"Query: {query}")
            logger.error(f"Traceback: {traceback.format_exc()}")
            raise

    @staticmethod
    def _build_blocks_brief_query(start_block: int, end_block: int, size: int) -> Dict[str, Any]:
        return {
            "query": {
                "range": {
                    "Number": {
                        "gte": start_block,
                        "lte": end_block
                    }
                }
            },
            "size": size,
            "_source": [
                "Number",
                "Hash",
                "Timestamp",
                "GasLimit",
                "GasUsed",
                "BaseFee",
                "Difficulty",
                "Miner",
                "ExtraData",
                "TxnCount",
                "BlobGasUsed",
                "ExcessBlobGas"
            ],
            "sort": [{"Number": {"order": "asc"}}]
        }


    @retry(
        stop=stop_after_attempt(5),
        wait=wait_exponential(multiplier=1, min=4, max=10),
        retry=retry_if_exception_type(ConnectionTimeout)
    )
    async def get_blocks_brief(self, start_block: int, end_block: int, size: int = 1000) -> List[Dict[str, Any]]:
        query = self._build_blocks_brief_query(start_block, end_block, size)
        
        try:
            response = await self.client.search(index="eth_block", body=query, scroll='2m')
            scroll_id = response['_scroll_id']
            hits = response['hits']['hits']

            while len(response['hits']['hits']) > 0:
                response = await self.client.scroll(scroll_id=scroll_id, scroll='2m')
                scroll_id = response['_scroll_id']
                hits.extend(response['hits']['hits'])

            blocks = []
            for hit in hits:
                block = hit['_source']
                blocks.append({
                    'block_number': block['Number'],
                    'block_hash': block['Hash'],
                    'timestamp': block['Timestamp'],
                    'gas_limit': block['GasLimit'],
                    'gas_used': block['GasUsed'],
                    'base_fee': block.get('BaseFee'),
                    'difficulty': block.get('Difficulty'),
                    'miner': block['Miner'],
                    'extra_data': block.get('ExtraData'),
                    'transaction_count': block.get('TxnCount'),
                    'blob_gas_used': block.get('BlobGasUsed'),
                    'excess_blob_gas': block.get('ExcessBlobGas')
                })
            return blocks

        except ConnectionTimeout as e:
            logger.error(f"Connection timeout occurred: {e}. Retrying...")
            raise
        except OpenSearchException as e:
            logger.error(f"OpenSearch exception occurred: {e}")
            raise
        finally:
            if 'scroll_id' in locals():
                await self.client.clear_scroll(scroll_id=scroll_id)