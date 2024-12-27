from typing import List, Dict, Any, Optional
import asyncio
import logging
from urllib.parse import urlparse
import traceback
import time

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
        self._last_request_time = 0
        self._requests_per_second = 8
        self._rate_limiter = asyncio.Semaphore(self._requests_per_second)
        self._batch_size = 500

    async def __aenter__(self):
        """Async context manager entry"""
        await super().__aenter__()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit"""
        try:
            await self.close()
        finally:
            await super().__aexit__(exc_type, exc_val, exc_tb)

    async def close(self):
        """Close the client and cleanup resources"""
        if hasattr(self, 'client') and self.client is not None:
            await self.client.close()
            self.client = None
        await super().close()

    async def _rate_limited_search(self, **kwargs):
        """
        Execute a rate-limited search request.
        Ensures we don't exceed the specified requests per second limit.
        """
        current_time = time.time()
        time_since_last_request = current_time - self._last_request_time
        
        if time_since_last_request < 1.0 / self._requests_per_second:
            await asyncio.sleep(1.0 / self._requests_per_second - time_since_last_request)
        
        try:
            async with self._rate_limiter:
                result = await self.client.search(**kwargs)
            self._last_request_time = time.time()
            return result
        except Exception as e:
            logger.error(f"Error in rate limited search: {str(e)}")
            raise

    async def search(self, **kwargs):
        """
        Rate-limited search method that wraps the AsyncOpenSearch search method.
        All parameters are passed directly to the underlying search method.
        """
        return await self._rate_limited_search(**kwargs)

    async def _rate_limited_scroll(self, scroll_id: str, scroll: str = '2m') -> Dict:
        """Rate-limited scroll operation"""
        try:
            async with self._rate_limiter:
                return await self.client.scroll(scroll_id=scroll_id, scroll=scroll)
        except Exception as e:
            logger.error(f"Error in rate limited scroll: {str(e)}")
            raise

    async def _rate_limited_clear_scroll(self, scroll_id: str) -> Dict:
        """Rate-limited clear scroll operation"""
        try:
            async with self._rate_limiter:
                return await self.client.clear_scroll(scroll_id=scroll_id)
        except Exception as e:
            logger.error(f"Error in rate limited clear scroll: {str(e)}")
            raise

    @retry(
        stop=stop_after_attempt(5),
        wait=wait_exponential(multiplier=1, min=4, max=10),
        retry=retry_if_exception_type((ConnectionTimeout, OpenSearchException)),
        reraise=True
    )
    async def search_transaction_batch(self, batch_hashes: List[str], index: str = "eth_block") -> Dict:
        """
        Search for a batch of transaction hashes with rate limiting and scroll support.
        Uses parallel processing for better performance.
        
        Args:
            batch_hashes: List of transaction hashes to search for
            index: OpenSearch index to search in (default: "eth_block")
            
        Returns:
            Dict containing the search results with all matching transactions
        """
        # Split hashes into smaller batches
        batches = [batch_hashes[i:i + self._batch_size] for i in range(0, len(batch_hashes), self._batch_size)]
        
        # Process batches in parallel
        tasks = []
        for batch in batches:
            tasks.append(self._search_batch(batch, index))
        
        # Wait for all batches to complete
        batch_results = await asyncio.gather(*tasks)
        
        # Combine results
        all_hits = []
        for result in batch_results:
            if result and 'hits' in result and 'hits' in result['hits']:
                all_hits.extend(result['hits']['hits'])
        
        return {
            'hits': {
                'hits': all_hits,
                'total': len(all_hits)
            }
        }

    async def _search_batch(self, hashes: List[str], index: str) -> Dict:
        """
        Search for a single batch of transaction hashes efficiently.
        
        Args:
            hashes: List of transaction hashes to search for
            index: OpenSearch index to search in
            
        Returns:
            Dict containing the search results for this batch
        """
        query = {
            "size": len(hashes),
            "_source": False,  # Don't fetch the _source field at all
            "query": {
                "nested": {
                    "path": "Transactions",
                    "query": {
                        "terms": {
                            "Transactions.Hash": hashes
                        }
                    },
                    "inner_hits": {
                        "_source": [
                            "Transactions.Hash",
                            "Transactions.FromAddress",
                            "Transactions.ToAddress",
                            "Transactions.Value",
                            "Transactions.Status",
                            "Transactions.Logs",
                            "Transactions.CallFunction",
                            "Transactions.CallParameter",
                            "Transactions.BalanceWrite"
                        ],
                        "size": len(hashes)
                    }
                }
            }
        }

        try:
            # Direct search without scrolling
            response = await self._rate_limited_search(index=index, body=query)
            
            # Return results directly
            return {
                'hits': {
                    'hits': response['hits']['hits'],
                    'total': len(response['hits']['hits'])
                }
            }

        except Exception as e:
            logger.error(f"Error searching transaction batch: {str(e)}")
            return None

    async def search_logs(self, index: str, start_block: int, end_block: int, 
                          event_topics: List[str], size: int = 1000, address: Optional[str] = None) -> List[Dict[str, Any]]:
        query = self._build_query(start_block, end_block, event_topics, size, address)
        # print(query)
        
        try:
            response = await self.client.search(index=index, body=query, scroll='2m')
            scroll_id = response['_scroll_id']
            hits = response['hits']['hits']

            while len(response['hits']['hits']) > 0:
                response = await self._rate_limited_scroll(scroll_id=scroll_id)
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
                await self._rate_limited_clear_scroll(scroll_id=scroll_id)

    @staticmethod
    def _build_query(start_block: int, end_block: int, event_topics: List[str], size: int, address: Optional[str] = None) -> Dict[str, Any]:
        must_conditions = [
            {"range": {"Number": {"gte": start_block, "lte": end_block}}},
            {"nested": {
                "path": "Transactions.Logs",
                "query": {
                    "bool": {
                        "must": [
                            {"terms": {"Transactions.Logs.Topics": event_topics}}
                        ]
                    }
                },
                "inner_hits": {
                    "_source": [
                        # "Transactions.Logs.Topics",
                        # "Transactions.Logs.Data",
                        # "Transactions.Logs.Address",
                        # "transactions.logs.Revert",
                    ],
                    "size": 100000
                }
            }}
        ]
        
        # Add address filter if provided
        if address:
            must_conditions[1]["nested"]["query"]["bool"]["must"].append(
                {"term": {"Transactions.Logs.Address": address.lower()}}
            )

        return {
            "query": {
                "bool": {
                    "must": must_conditions
                }
            },
            "size": size,
            "_source": ["Number", "Transactions.Hash", "Transactions.FromAddress", "Transactions.ToAddress"],
            "sort": [{"Number": {"order": "asc"}}]
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
                response = await self._rate_limited_scroll(scroll_id=scroll_id)
                scroll_id = response['_scroll_id']

            await self._rate_limited_clear_scroll(scroll_id=scroll_id)
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
                response = await self._rate_limited_scroll(scroll_id=scroll_id)
                scroll_id = response['_scroll_id']

            await self._rate_limited_clear_scroll(scroll_id=scroll_id)
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
    

 
    async def get_eth_change_in(self, tx_hash):
        try:
            response = await self.client.get(index="eth_block", id=tx_hash)
            return response['_source']['EthChangeIn']
        except RequestError as e:
            logger.error(f"OpenSearch request error: {e}")
            logger.error(f"Query: {query}")
            logger.error(f"Error details: {e.info}")
            raise
        except TransportError as e:
            logger.error(f"OpenSearch transport error: {e}")
            logger.error(f"Query: {query}")
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
            response = await self._rate_limited_search(index="eth_block", body=query, scroll='2m')
            scroll_id = response['_scroll_id']
            hits = response['hits']['hits']

            while len(response['hits']['hits']) > 0:
                response = await self._rate_limited_scroll(scroll_id=scroll_id)
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
                await self._rate_limited_clear_scroll(scroll_id=scroll_id)

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=4, max=10),
        retry=retry_if_exception_type((ConnectionTimeout, OpenSearchException))
    )
    async def get_contract_creator_tx(self, contract_address: str) -> Optional[str]:
        """
        Get the transaction hash that created a contract.
        
        Args:
            contract_address: The contract address to look up
            
        Returns:
            Optional[str]: The transaction hash that created the contract, or None if not found
        """
        try:
            query = {
                "query": {
                    "nested": {
                        "path": "Transactions.Contracts",
                        "query": {
                            "term": {
                                "Transactions.Contracts.Address": {
                                    "value": contract_address.lower()
                                }
                            }
                        },
                        "inner_hits": {
                            "_source": {
                                "includes": ["Transactions.Contracts.*", "Transactions.Hash"]
                            },
                            "size": 10
                        }
                    }
                }
            }
            
            response = await self.client.search(
                index="eth_code_all",
                body=query
            )
            
            if response["hits"]["total"]["value"] > 0:
                # Get the transaction that contains the contract creation
                transaction = response["hits"]["hits"][0]["_source"]["Transactions"]
                
                # Find the specific transaction that created our contract
                for tx in transaction:
                    if "Contracts" in tx:
                        for contract in tx["Contracts"]:
                            if contract["Address"].lower() == contract_address.lower():
                                return tx["Hash"]
            
            return None
            
        except NotFoundError:
            logger.error(f"Index eth_code_all not found")
            return None
        except Exception as e:
            logger.error(f"Error getting contract creator tx: {str(e)}")
            logger.error(traceback.format_exc())
            return None

    async def get_eth_transfers(self, 
                                start_block: int = None,
                                end_block: int = None,
                                from_address: str = None,
                                to_address: str = None,
                                min_value: str = None,
                                max_value: str = None,
                                size: int = 100) -> List[Dict[str, Any]]:
        """
        Retrieve Ethereum transaction and internal transaction data that matches 
        optional filtering criteria and return a flat list of transfers.

        Parameters
        ----------
        start_block : int, optional
            The starting block number (inclusive).
        end_block : int, optional
            The ending block number (inclusive).
        from_address : str, optional
            Filter transactions by this "FromAddress".
        to_address : str, optional
            Filter transactions by this "ToAddress".
        min_value : str, optional
            Minimum value in Wei
        max_value : str, optional
            Maximum value in Wei
        size : int, optional
            The maximum number of blocks to retrieve.

        Returns
        -------
        List[Dict[str, Any]]
            A flat list of dictionaries, each containing:
            - BlockNumber (int)
            - BlockTimestamp (str)
            - Hash (str) : For internal txns, this is the "Id" field
            - FromAddress (str)
            - ToAddress (str)
            - Value (str)
        """
        
        # Build the range query for blocks if provided
        must_clauses = []
        if start_block is not None or end_block is not None:
            block_range = {}
            if start_block is not None:
                block_range["gte"] = start_block
            if end_block is not None:
                block_range["lte"] = end_block
                
            must_clauses.append({
                "range": {
                    "Number": block_range
                }
            })
        
        # Conditions for Transactions
        transactions_must = [
            {"exists": {"field": "Transactions.FromAddress"}},
            {"exists": {"field": "Transactions.ToAddress"}},
            {"exists": {"field": "Transactions.Value"}}
        ]
        if from_address:
            transactions_must.append({"term": {"Transactions.FromAddress": from_address}})
        if to_address:
            transactions_must.append({"term": {"Transactions.ToAddress": to_address}})
        if min_value or max_value:
            value_range = {}
            if min_value:
                value_range["gte"] = min_value
            if max_value:
                value_range["lte"] = max_value
            transactions_must.append({"range": {"Transactions.Value": value_range}})

        # Conditions for Internal Transactions
        internal_txns_must = [
            {"exists": {"field": "Transactions.InternalTxns.FromAddress"}},
            {"exists": {"field": "Transactions.InternalTxns.ToAddress"}},
            {"exists": {"field": "Transactions.InternalTxns.Value"}}
        ]
        if from_address:
            internal_txns_must.append({"term": {"Transactions.InternalTxns.FromAddress": from_address}})
        if to_address:
            internal_txns_must.append({"term": {"Transactions.InternalTxns.ToAddress": to_address}})
        if min_value or max_value:
            value_range = {}
            if min_value:
                value_range["gte"] = min_value
            if max_value:
                value_range["lte"] = max_value
            internal_txns_must.append({"range": {"Transactions.InternalTxns.Value": value_range}})

        query = {
            "_source": ["Number", "Timestamp"],
            "size": size,
            "query": {
                "bool": {
                    "must": must_clauses,
                    "should": [
                        {
                            "nested": {
                                "path": "Transactions",
                                "query": {
                                    "bool": {
                                        "should": [
                                            {
                                                "bool": {
                                                    "must": transactions_must
                                                }
                                            },
                                            {
                                                "nested": {
                                                    "path": "Transactions.InternalTxns",
                                                    "query": {
                                                        "bool": {
                                                            "must": internal_txns_must
                                                        }
                                                    },
                                                    "inner_hits": {
                                                        "_source": [
                                                            "Transactions.InternalTxns.FromAddress",
                                                            "Transactions.InternalTxns.ToAddress",
                                                            "Transactions.InternalTxns.Value"
                                                        ],
                                                        "size": 10,
                                                        "name": "internal_txns"
                                                    }
                                                }
                                            }
                                        ],
                                        "minimum_should_match": 1
                                    }
                                },
                                "inner_hits": {
                                    "_source": [
                                        "Transactions.Hash"
                                    ],
                                    "size": 100,
                                    "name": "parent_txs"
                                }
                            }
                        }
                    ],
                    "minimum_should_match": 1
                }
            }
        }

        response = await self.client.search(index="eth_block", body=query)
        

        if response['hits']['hits']:
            first_hit = response['hits']['hits'][0]
            if 'inner_hits' in first_hit:
                for name, inner in first_hit['inner_hits'].items():
                    for hit in inner['hits']['hits']:
                        if 'inner_hits' in hit:
                            for inner_name, inner_hits in hit['inner_hits'].items():
                                logger.warning(f"Inner inner hits {inner_name}: {inner_hits['hits']['hits']}")

        transfers = []
        for hit in response['hits']['hits']:
            block_number = hit["_source"].get("Number")
            block_timestamp = hit["_source"].get("Timestamp")

            # Process parent transactions
            if "parent_txs" in hit.get("inner_hits", {}):
                parent_hits = hit["inner_hits"]["parent_txs"]["hits"]["hits"]
                
                for t_hit in parent_hits:
                    # Debug the structure
                    
                    parent_tx_hash = t_hit["_source"].get("Hash")
                    if not parent_tx_hash:
                        continue

                    # Add regular transaction
                    source = t_hit["_source"]
                    if all(key in source for key in ["FromAddress", "ToAddress", "Value"]):
                        transfers.append({
                            "BlockNumber": block_number,
                            "BlockTimestamp": block_timestamp,
                            "Hash": parent_tx_hash,
                            "FromAddress": source["FromAddress"],
                            "ToAddress": source["ToAddress"],
                            "Value": source["Value"],
                            "TokenAddress": "0xEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEE",
                            "IsInternal": False
                        })

                    # Add internal transactions if any
                    if "internal_txns" in t_hit.get("inner_hits", {}):
                        internal_hits = t_hit["inner_hits"]["internal_txns"]["hits"]["hits"]
                        
                        for i_hit in internal_hits:
                            # Debug the structure
                            
                            i = i_hit["_source"]
                            if not i:
                                continue
                            transfers.append({
                                "BlockNumber": block_number,
                                "BlockTimestamp": block_timestamp,
                                "Hash": parent_tx_hash,
                                "FromAddress": i["FromAddress"],
                                "ToAddress": i["ToAddress"],
                                "Value": i["Value"],
                                "TokenAddress": "0xEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEE",
                                "IsInternal": True
                            })
        logger.warning(f"Total transfers found: {len(transfers)}")
        return transfers

    async def get_eth_transfers_batch(self,
                                    batch_ranges: List[Dict[str, int]],
                                    from_address: str = None,
                                    to_address: str = None,
                                    min_value: str = None,
                                    max_value: str = None,
                                    size: int = 100,
                                    max_parallel: int = 5) -> List[Dict[str, Any]]:
        """
        Get ETH transfers in parallel for multiple block ranges.
        
        Parameters
        ----------
        batch_ranges : List[Dict[str, int]]
            List of dictionaries with 'start' and 'end' block numbers
        from_address : str, optional
            Filter by sender address
        to_address : str, optional
            Filter by recipient address
        min_value : str, optional
            Minimum value in Wei
        max_value : str, optional
            Maximum value in Wei
        size : int, optional
            Number of results per query
        max_parallel : int, optional
            Maximum number of parallel requests
            
        Returns
        -------
        List[Dict[str, Any]]
            Combined results from all batches
        """
        semaphore = asyncio.Semaphore(max_parallel)
        
        async def get_batch(batch_range: Dict[str, int]) -> List[Dict[str, Any]]:
            async with semaphore:
                return await self.get_eth_transfers(
                    start_block=batch_range['start'],
                    end_block=batch_range['end'],
                    from_address=from_address,
                    to_address=to_address,
                    min_value=min_value,
                    max_value=max_value,
                    size=size
                )
        
        # Create tasks for all batches
        tasks = [get_batch(batch) for batch in batch_ranges]
        
        # Wait for all tasks to complete
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        # Process results and handle any errors
        transfers = []
        for i, result in enumerate(results):
            if isinstance(result, Exception):
                logger.error(f"Error in batch {batch_ranges[i]}: {str(result)}")
                continue
            transfers.extend(result)
            
        return transfers

    async def get_eth_transfers_batched(self, 
                                start_block: int = None,
                                end_block: int = None,
                                from_address: str = None,
                                to_address: str = None,
                                min_value: str = None,
                                max_value: str = None,
                                size: int = 100) -> List[Dict[str, Any]]:
        """
        Retrieve Ethereum transaction and internal transaction data that matches 
        optional filtering criteria and return a flat list of transfers.

        Parameters
        ----------
        start_block : int, optional
            The starting block number (inclusive).
        end_block : int, optional
            The ending block number (inclusive).
        from_address : str, optional
            Filter transactions by this "FromAddress".
        to_address : str, optional
            Filter transactions by this "ToAddress".
        min_value : str, optional
            Minimum value in Wei
        max_value : str, optional
            Maximum value in Wei
        size : int, optional
            The maximum number of blocks to retrieve.

        Returns
        -------
        List[Dict[str, Any]]
            A flat list of dictionaries, each containing:
            - BlockNumber (int)
            - BlockTimestamp (str)
            - Hash (str) : For internal txns, this is the "Id" field
            - FromAddress (str)
            - ToAddress (str)
            - Value (str)
        """
        
        query = {
            "size": size,
            "_source": ["Number", "Timestamp"],
            "query": {
                "bool": {
                    "must": [
                        {"range": {"Number": {"gte": start_block, "lt": end_block}}},
                        {"nested": {
                            "path": "Transactions",
                            "query": {
                                "bool": {
                                    "should": [
                                        # Regular transactions
                                        {"bool": {"must": [
                                            {"exists": {"field": "Transactions.Hash"}},
                                            {"exists": {"field": "Transactions.FromAddress"}},
                                            {"exists": {"field": "Transactions.ToAddress"}},
                                            {"exists": {"field": "Transactions.Value"}}
                                        ]}},
                                        # Internal transactions
                                        {"nested": {
                                            "path": "Transactions.InternalTxns",
                                            "query": {
                                                "bool": {"must": [
                                                    {"exists": {"field": "Transactions.InternalTxns.FromAddress"}},
                                                    {"exists": {"field": "Transactions.InternalTxns.ToAddress"}},
                                                    {"exists": {"field": "Transactions.InternalTxns.Value"}}
                                                ]}
                                            }
                                        }}
                                    ]
                                }
                            },
                            "inner_hits": {
                                "_source": ["Hash", "FromAddress", "ToAddress", "Value"],
                                "size": 10000,
                                "inner_hits": {
                                    "InternalTxns": {
                                        "path": "Transactions.InternalTxns",
                                        "_source": ["FromAddress", "ToAddress", "Value"],
                                        "size": 10000
                                    }
                                }
                            }
                        }}
                    ]
                }
            }
        }

        # Add filters if provided
        if from_address:
            query["query"]["bool"]["must"].append({
                "nested": {
                    "path": "Transactions",
                    "query": {
                        "bool": {
                            "should": [
                                {"term": {"Transactions.FromAddress": from_address}},
                                {"nested": {
                                    "path": "Transactions.InternalTxns",
                                    "query": {"term": {"Transactions.InternalTxns.FromAddress": from_address}}
                                }}
                            ]
                        }
                    }
                }
            })

        if to_address:
            query["query"]["bool"]["must"].append({
                "nested": {
                    "path": "Transactions",
                    "query": {
                        "bool": {
                            "should": [
                                {"term": {"Transactions.ToAddress": to_address}},
                                {"nested": {
                                    "path": "Transactions.InternalTxns",
                                    "query": {"term": {"Transactions.InternalTxns.ToAddress": to_address}}
                                }}
                            ]
                        }
                    }
                }
            })

        response = await self.client.search(index="eth_block", body=query)
        
        # Debug logging
        if response['hits']['hits']:
            first_hit = response['hits']['hits'][0]
            if 'inner_hits' in first_hit:
                for name, inner in first_hit['inner_hits'].items():
                    for hit in inner['hits']['hits']:
                        if 'inner_hits' in hit:
                            for inner_name, inner_hits in hit['inner_hits'].items():
                                logger.warning(f"Inner inner hits {inner_name}: {inner_hits['hits']['hits']}")

        transfers = []
        for hit in response['hits']['hits']:
            block_number = hit["_source"].get("Number")
            block_timestamp = hit["_source"].get("Timestamp")
            logger.warning(f"Processing block {block_number}")

            # Process transactions
            if "Transactions" in hit.get("inner_hits", {}):
                tx_hits = hit["inner_hits"]["Transactions"]["hits"]["hits"]
                logger.warning(f"Found {len(tx_hits)} transactions in block {block_number}")
                
                for t_hit in tx_hits:
                    logger.warning(f"Raw transaction hit: {t_hit}")
                    source = t_hit.get("_source", {})
                    tx_hash = source.get("Hash")
                    if not tx_hash:
                        logger.warning(f"Missing Hash in transaction: {source}")
                        continue

                    logger.warning(f"Processing transaction {tx_hash}")
                    logger.warning(f"Transaction source: {source}")

                    # Add regular transaction if it has all required fields
                    if all(key in source for key in ["FromAddress", "ToAddress", "Value"]):
                        transfer = {
                            "BlockNumber": block_number,
                            "BlockTimestamp": block_timestamp,
                            "Hash": tx_hash,
                            "FromAddress": source["FromAddress"],
                            "ToAddress": source["ToAddress"],
                            "Value": source["Value"],
                            "TokenAddress": "0xEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEE",
                            "IsInternal": False
                        }
                        transfers.append(transfer)
                    else:
                        logger.warning(f"Regular transfer missing required fields: {source}")

                    # Add internal transactions if any
                    if "InternalTxns" in t_hit.get("inner_hits", {}):
                        internal_hits = t_hit["inner_hits"]["InternalTxns"]["hits"]["hits"]
                        
                        for i_hit in internal_hits:
                            i = i_hit.get("_source", {})
                            
                            if all(key in i for key in ["FromAddress", "ToAddress", "Value"]):
                                transfer = {
                                    "BlockNumber": block_number,
                                    "BlockTimestamp": block_timestamp,
                                    "Hash": tx_hash,
                                    "FromAddress": i["FromAddress"],
                                    "ToAddress": i["ToAddress"],
                                    "Value": i["Value"],
                                    "TokenAddress": "0xEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEE",
                                    "IsInternal": True
                                }
                                transfers.append(transfer)
                                logger.warning(f"Added internal transfer: {transfer}")
                            else:
                                logger.warning(f"Internal transfer missing required fields: {i}")
        logger.warning(f"Total transfers found: {len(transfers)}")
        return transfers

    async def get_erc20_transfers(self,
                                start_block: int = None,
                                end_block: int = None,
                                token_address: str = None,
                                from_address: str = None,
                                to_address: str = None,
                                size: int = 1000) -> List[Dict[str, Any]]:
        """
        Retrieve ERC20 token transfer events.

        Parameters
        ----------
        start_block : int, optional
            The starting block number (inclusive).
        end_block : int, optional
            The ending block number (inclusive).
        token_address : str, optional
            Filter transfers by this token contract address.
        from_address : str, optional
            Filter transfers by sender address.
        to_address : str, optional
            Filter transfers by recipient address.
        size : int, optional
            The maximum number of results to retrieve.

        Returns
        -------
        List[Dict[str, Any]]
            A list of dictionaries, each containing:
            - BlockNumber (int)
            - Hash (str)
            - FromAddress (str)
            - ToAddress (str)
            - Value (str)
            - TokenAddress (str)
        """
        # ERC20 Transfer event topic
        transfer_topic = "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"
        
        # Build topic filters
        topics = [transfer_topic]  # First topic is always the event signature
        
        # Add from_address if provided
        if from_address:
            # Pad address to 32 bytes for topic matching
            topics.append("0x" + from_address[2:].lower().zfill(64))
            
            # Add to_address only if from_address is also provided
            if to_address:
                topics.append("0x" + to_address[2:].lower().zfill(64))
        elif to_address:
            # If only to_address provided, we'll filter it post-query
            topics.extend([None])  # Add null for from topic position
        
        # Get all matching logs
        logs = await self.search_logs(
            index="eth_block",
            start_block=start_block,
            end_block=end_block,
            event_topics=topics,
            size=size
        )

        transfers = []
        for block in logs:
            block_number = block['_source'].get('Number')
            
            # Process each transaction in the block
            for tx in block['_source'].get('Transactions', []):
                tx_hash = tx.get('Hash')
                
                # Process each log in the transaction
                for log in tx.get('Logs', []):
                    # Skip if not a Transfer event
                    if not log.get('Topics') or log['Topics'][0] != transfer_topic:
                        continue
                        
                    # Skip if not from the specified token
                    if token_address and log.get('Address', '').lower() != token_address.lower():
                        continue
                    
                    # Extract addresses from topics
                    topics = log.get('Topics', [])
                    if len(topics) >= 3:
                        from_addr = "0x" + topics[1][-40:]
                        to_addr = "0x" + topics[2][-40:]
                        
                        # Skip if addresses don't match filters
                        if from_address and from_addr.lower() != from_address.lower():
                            continue
                        if to_address and to_addr.lower() != to_address.lower():
                            continue
                        
                        # Convert value from hex
                        value = log.get('Data', '0x0')
                        if value.startswith('0x'):
                            value = str(int(value, 16))
                        
                        transfer = {
                            'BlockNumber': block_number,
                            'Hash': tx_hash,
                            'FromAddress': from_addr,
                            'ToAddress': to_addr,
                            'Value': value,
                            'TokenAddress': log.get('Address')
                        }
                        transfers.append(transfer)

        return transfers

    
    async def get_native_balance_changes(self, tx_hash: str) -> List[Dict[str, Any]]:
        """
        Get native token balance changes for a specific transaction.
        
        Args:
            tx_hash (str): Transaction hash to query
            
        Returns:
            List[Dict[str, Any]]: List of balance changes, each containing:
                - address: The affected address
                - prev_balance: Previous balance
                - current_balance: Current balance
                - difference: Balance difference (positive for increase, negative for decrease)
        """
        query = {
            "_source": False,
            "query": {
                "nested": {
                    "path": "Transactions",
                    "query": {
                        "term": {
                            "Transactions.Hash": {
                                "value": tx_hash
                            }
                        }
                    },
                    "inner_hits": {
                        "name": "matching_transactions",
                        "size": 10,
                        "_source": {
                            "includes": ["Transactions.BalanceWrite"]
                        }
                    }
                }
            }
        }

        try:
            # Search across all eth_block_* indices
            response = await self.client.search(
                index="eth_block_*",
                body=query
            )

            balance_changes = []
            if response["hits"]["total"]["value"] > 0:
                # Get the first matching transaction's balance writes
                inner_hits = response["hits"]["hits"][0]["inner_hits"]["matching_transactions"]["hits"]["hits"]
                if inner_hits:
                    balance_writes = inner_hits[0]["_source"]["BalanceWrite"]
                    
                    for write in balance_writes:
                        prev_balance = int(write["Prev"])
                        current_balance = int(write["Current"])
                        balance_changes.append({
                            "address": write["Address"],
                            "prev_balance": prev_balance,
                            "current_balance": current_balance,
                            "difference": current_balance - prev_balance
                        })

            return balance_changes

        except Exception as e:
            logger.error(f"Error fetching balance changes for tx {tx_hash}: {str(e)}")
            raise

    async def get_native_balance_changes_batch(self, tx_hashes: List[str], batch_size: int = 1000) -> Dict[str, Dict[str, Any]]:
        """
        Get native token balance changes grouped by address, with consecutive change tracking.
        
        Args:
            tx_hashes (List[str]): List of transaction hashes to query
            batch_size (int, optional): Maximum number of transactions per query. Defaults to 1000.
            
        Returns:
            Dict[str, Dict[str, Any]]: Dictionary mapping addresses to their changes:
                - total_change: Total balance change across all transactions
                - changes_count: Number of times the balance changed
                - is_consecutive: True if all changes follow each other (no gaps in balance history)
                - first_balance: First recorded balance
                - last_balance: Last recorded balance
                - tx_hashes: List of transaction hashes affecting this address
        """
        if not tx_hashes:
            return {}

        address_changes = {}
        
        for i in range(0, len(tx_hashes), batch_size):
            batch = tx_hashes[i:i + batch_size]
            
            query = {
                "_source": False,
                "query": {
                    "nested": {
                        "path": "Transactions",
                        "query": {
                            "terms": {
                                "Transactions.Hash": batch
                            }
                        },
                        "inner_hits": {
                            "name": "matching_transactions",
                            "size": len(batch),
                            "_source": {
                                "includes": ["Transactions.Hash", "Transactions.BalanceWrite"]
                            }
                        }
                    }
                },
                "size": len(batch)
            }

            try:
                response = await self.client.search(
                    index="eth_block_*",
                    body=query
                )

                if response["hits"]["total"]["value"] > 0:
                    for hit in response["hits"]["hits"]:
                        inner_hits = hit["inner_hits"]["matching_transactions"]["hits"]["hits"]
                        
                        for inner_hit in inner_hits:
                            tx_hash = inner_hit["_source"]["Hash"]
                            balance_writes = inner_hit["_source"].get("BalanceWrite", [])
                            
                            for write in balance_writes:
                                address = write["Address"]
                                prev_balance = int(write["Prev"])
                                current_balance = int(write["Current"])
                                difference = current_balance - prev_balance

                                if address not in address_changes:
                                    address_changes[address] = {
                                        "total_change": difference,
                                        "changes_count": 1,
                                        "is_consecutive": True,
                                        "first_balance": prev_balance,
                                        "last_balance": current_balance,
                                        "_balance_history": {prev_balance, current_balance},
                                        "tx_hashes": [tx_hash]
                                    }
                                else:
                                    changes = address_changes[address]
                                    changes["total_change"] += difference
                                    changes["changes_count"] += 1
                                    changes["last_balance"] = current_balance
                                    changes["tx_hashes"].append(tx_hash)
                                    
                                    # Track balance history to check consecutiveness
                                    if prev_balance not in changes["_balance_history"]:
                                        changes["is_consecutive"] = False
                                    changes["_balance_history"].add(prev_balance)
                                    changes["_balance_history"].add(current_balance)

            except Exception as e:
                logger.error(f"Error fetching batch balance changes for batch {i//batch_size + 1}: {str(e)}")
                raise

        # Remove temporary balance history from results
        for changes in address_changes.values():
            changes.pop("_balance_history")
            # Remove duplicates and maintain order
            changes["tx_hashes"] = list(dict.fromkeys(changes["tx_hashes"]))

        return address_changes
