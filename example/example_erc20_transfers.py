import asyncio
from itertools import count
import logging
import sys
import os
from datetime import datetime
from typing import List, Dict, Any
import time

# Add the project root directory to the Python path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from web3_data_center.clients.opensearch_client import OpenSearchClient
from web3_data_center.clients.database.postgresql_client import PostgreSQLClient
from web3_data_center.utils.logger import get_logger

logger = get_logger(__name__)
logger.setLevel(logging.INFO)

# Constants
QUERY_SIZE = 1000  # Number of events per query
BLOCK_PADDING = 1000000  # Number of blocks to pad before and after trade blocks
MAX_RETRIES = 3  # Maximum number of retries for failed queries
RATE_LIMIT = 5  # Maximum number of queries per second
RATE_LIMIT_WINDOW = 1.0  # Time window in seconds

class RateLimiter:
    def __init__(self, rate: int, window: float):
        self.rate = rate
        self.window = window
        self.tokens = rate
        self.last_update = time.monotonic()

    async def acquire(self):
        while self.tokens <= 0:
            now = time.monotonic()
            time_passed = now - self.last_update
            self.tokens = min(self.rate, self.tokens + time_passed * (self.rate / self.window))
            self.last_update = now
            if self.tokens <= 0:
                await asyncio.sleep(self.window / self.rate)
        self.tokens -= 1

async def get_scam_tokens(pg_client: PostgreSQLClient) -> List[Dict[str, Any]]:
    """Get potential scam tokens from banana_gun_tokens table."""
    query = """
    SELECT 
        token_address,
        first_trade_block,
        last_trade_block
    FROM banana_gun_tokens 
    WHERE is_potential_scam = TRUE
    """
    try:
        result = await pg_client.execute(query)
        if result is None:
            logger.error("Query returned None from database")
            return []
        logger.info(f"Found {len(result)} potential scam tokens in database")
        return result
    except Exception as e:
        logger.error(f"Error querying scam tokens: {e}")
        return []

async def get_missing_token_transfers(pg_client: PostgreSQLClient) -> List[Dict[str, Any]]:
    """Get tokens that are missing from token_transfers table."""
    query = """
    SELECT s.*
    FROM scam_summary s
    WHERE NOT EXISTS (
        SELECT 1
        FROM token_transfers t
        WHERE LOWER(s.token_address) = t.token_address
    );
    """
    try:
        result = await pg_client.execute(query)
        if result is None:
            logger.error("Query returned None from database")
            return []
        logger.info(f"Found {len(result)} tokens missing transfers")
        return result
    except Exception as e:
        logger.error(f"Error querying missing token transfers: {e}")
        return []

def extract_transfer_from_log(log):
    """Extract transfer details from a log entry."""
    # print(log)
    transfers = []
    
    if '_source' not in log:
        print("No _source field in log")
        return []
        
    block_number = log['_source'].get('Number')
    if not block_number:
        print("No block number in log")
        return []
        
    # Get inner hits which contain the transfer logs
    inner_hits = log.get('inner_hits', {}).get('Transactions.Logs', {}).get('hits', {}).get('hits', [])
    if not inner_hits:
        print("No inner hits found")
        return []
        
    # Create a map of transaction index to transaction details for quick lookup
    transactions = log['_source'].get('Transactions', [])
    # print(len(inner_hits))
    for inner_hit in inner_hits:
        try:
            log_data = inner_hit.get('_source', {})
            topics = log_data.get('Topics', [])
            reverted = log_data.get('Revert')
            # print("reverted", reverted)
            if (reverted):
                continue
            
            # Ensure we have enough topics for a transfer event (topic[0] is the event signature)
            if len(topics) != 3:
                with open('transfer_log_errors.txt', 'a') as error_file:
                    error_file.write(f"Invalid number of topics in log: {block_number} - {log_data.get('Address', '').lower()} - {topics}\n")
                continue
                
            # Extract transfer data
            from_address = '0x' + topics[1][-40:]  # Remove padding and add 0x prefix
            to_address = '0x' + topics[2][-40:]    # Remove padding and add 0x prefix
            
            # Get amount from data field (remove 0x prefix and convert from hex)
            amount_hex = log_data.get('Data', '0x0')
            if amount_hex.startswith('0x'):
                amount_hex = amount_hex[2:]
            amount = int(amount_hex, 16)
            
            # Get the parent transaction path and extract transaction index
            nested_info = inner_hit.get('_nested', {})
            if not nested_info:
                with open('transfer_log_errors.txt', 'a') as error_file:
                    error_file.write(f"No nested info found: {block_number} - {log_data.get('Address', '').lower()} - {topics}\n")
                continue
                
            # Extract transaction index from nested path (format: Transactions.{index}.Logs)
            tx_index = nested_info.get('offset', None)
            if tx_index is None or tx_index >= len(transactions):
                with open('transfer_log_errors.txt', 'a') as error_file:
                    error_file.write(f"Invalid transaction index: {block_number} - {log_data.get('Address', '').lower()} - {topics}\n")
                continue

            nested_log_info = nested_info.get('_nested', {})
            if not nested_log_info:
                with open('transfer_log_errors.txt', 'a') as error_file:
                    error_file.write(f"No nested log info found: {block_number} - {log_data.get('Address', '').lower()} - {topics}\n")
                continue

            log_index = nested_log_info.get('offset')
                
            # Get transaction context
            transaction = transactions[tx_index]
            tx_hash = transaction.get('Hash', '').lower()
            tx_from = transaction.get('FromAddress', '').lower()
            tx_to = transaction.get('ToAddress', '').lower()
            # print(to_address)
            # if not all([tx_hash, tx_from, tx_to]):
            #     print(tx_hash, tx_from, tx_to)
            #     print("Invalid transaction context")
            #     continue
                
            # Create transfer record with both log data and transaction context
            transfer = {
                'block_number': block_number,
                'transaction_hash': tx_hash,
                'log_index': log_index,
                'token_address': log_data.get('Address', '').lower(),
                'from_address': from_address.lower(),
                'to_address': to_address.lower(),
                'amount': str(amount),  # Convert to string to handle large numbers
                'value': str(amount*0.5),  # Convert to string to handle large numbers 
                'tx_from': tx_from,
                'tx_to': tx_to
            }
            
            transfers.append(transfer)
            
        except Exception as e:
            logger.error(f"Error processing transfer log: {e}")
            continue
    
    if transfers:
        logger.info(f"Successfully extracted {len(transfers)} transfers from block {block_number}")
    else:
        logger.info(f"No transfers found in block {block_number}")
    return transfers

async def store_transfers_batch(pg_client, transfers):
    """Store a batch of transfers in the database."""
    if not transfers:
        return
        
    # Create table if not exists
    await pg_client.execute("""
        CREATE TABLE IF NOT EXISTS token_transfers (
            block_number BIGINT,
            transaction_hash VARCHAR(66),
            log_index BIGINT,
            token_address VARCHAR(42),
            from_address VARCHAR(42),
            to_address VARCHAR(42),
            amount TEXT,
            value TEXT,
            tx_from VARCHAR(42),
            tx_to VARCHAR(42),
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (transaction_hash, log_index)
        );
    """)

    try:
        # Build the batch insert query
        values_template = "(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
        values_list = []
        params = []
        
        # Prepare values and parameters
        for transfer in transfers:
            values_list.append(values_template)
            params.extend([
                transfer['block_number'],
                transfer['transaction_hash'],
                transfer['log_index'],
                transfer['token_address'],
                transfer['from_address'],
                transfer['to_address'],
                transfer['amount'],
                transfer['value'],
                transfer['tx_from'],
                transfer['tx_to']
            ])
        
        # Construct the full query
        query = """
            INSERT INTO token_transfers (
                block_number, transaction_hash, log_index, token_address, 
                from_address, to_address, amount, value, tx_from, tx_to
            ) VALUES {} 
            
            ON CONFLICT (transaction_hash, log_index) DO NOTHING
        """.format(",".join(values_list))
        
        # Execute batch insert
        await pg_client.execute(query, params)
        logger.info(f"Successfully stored {len(transfers)} transfers in batch")
    except Exception as e:
        logger.error(f"Error batch inserting transfers: {e}")

async def process_token_transfers(
    token: Dict[str, Any], 
    pg_client: PostgreSQLClient,
    os_client: OpenSearchClient,
    rate_limiter: RateLimiter
) -> None:
    """Process transfers for a single token within its trading block range."""
    token_address = token['token_address'].lower()  # Convert to lowercase
    start_block = max(0, token['first_trade_block'] - BLOCK_PADDING)
    end_block = token['last_trade_block'] + BLOCK_PADDING
    
    logger.info(f"Processing transfers for token {token_address} "
               f"from block {start_block} to {end_block}")
    
    # Define topics to search
    topics = [
        '0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef',  # Standard ERC20 Transfer
        '0xe59fdd36d0d223c0c7d996db7ad796880f45e1936cb0bb7ac102e7082e031487'   # Alternative Transfer
    ]
    
    try:
        found_logs = False
        for topic in topics:
            if found_logs:
                break

            for retry in range(MAX_RETRIES):
                try:
                    # Wait for rate limiter
                    await rate_limiter.acquire()
                    
                    # Search directly using the client
                    logs = await os_client.search_logs(
                        index='eth_block',
                        event_topics=[topic],
                        start_block=start_block,
                        end_block=end_block,
                        address=token_address,
                        size=QUERY_SIZE
                    )
                    if logs:
                        logger.info(f"Extracted {len(logs)} transfers for token {token_address} with topic {topic}")
                        transfers = []
                        for log in logs:
                            log_transfers = extract_transfer_from_log(log)
                            if log_transfers:
                                transfers.extend(log_transfers)
                        if transfers:
                            await store_transfers_batch(pg_client, transfers)
                            found_logs = True
                            break  # Exit retry loop after finding logs
                    else:
                        logger.warning(f"No transfers found for token {token_address} with topic {topic}")
                    break  # Exit retry loop if no error occurred
                
                except Exception as e:
                    if retry < MAX_RETRIES - 1:
                        wait_time = (retry + 1) * 2  # Exponential backoff
                        logger.warning(f"Retry {retry + 1}/{MAX_RETRIES} for token {token_address} after {wait_time}s: {e}")
                        await asyncio.sleep(wait_time)
                    else:
                        raise

        if not found_logs:
            logger.warning(f"No transfers found for token {token_address} with any topic")

    except Exception as e:
        logger.error(f"Error processing transfers for token {token_address}: {e}")
        raise

async def main():
    """Main function to process transfers for all scam tokens."""
    config_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'config.yml')
    os_client = OpenSearchClient(config_path=config_path)
    pg_client = PostgreSQLClient(config_path=config_path, db_section='zju')
    rate_limiter = RateLimiter(RATE_LIMIT, RATE_LIMIT_WINDOW)
    
    try:
        async with os_client:
            # Process missing token transfers
            missing_tokens = await get_missing_token_transfers(pg_client)
            for token_data in missing_tokens:
                token = {
                    'token_address': token_data['token_address'],
                    'first_trade_block': token_data.get('first_block', 0),  # Adjust based on your data
                    'last_trade_block': 21000000  # Get latest block if not available
                }
                try:
                    await process_token_transfers(token, pg_client, os_client, rate_limiter)
                except Exception as e:
                    logger.error(f"Failed to process token {token['token_address']}: {e}")
                    continue
                
            logger.info("Completed processing missing token transfers")
            
            # # Get all scam tokens
            # tokens = await get_scam_tokens(pg_client)
            # if not tokens:
            #     logger.error("No tokens returned from get_scam_tokens")
            #     return
                
            # logger.info(f"Found {len(tokens)} potential scam tokens")
            
            # # Process each token
            # for i, token in enumerate(tokens, 1):
            #     # if i <= 2726: continue
            #     try:
            #         logger.info(f"Processing token {i}/{len(tokens)}: {token['token_address']}")
            #         await process_token_transfers(token, pg_client, os_client, rate_limiter)
            #     except Exception as e:
            #         logger.error(f"Error processing token {token['token_address']}: {e}")
            #         continue  # Continue with next token even if one fails
                
    except KeyboardInterrupt:
        logger.info("Interrupted by user")
    except Exception as e:
        logger.error(f"Error in main function: {e}")
        raise
    finally:
        await pg_client.close()

if __name__ == "__main__":
    logging.basicConfig(
        level=logging.DEBUG,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    asyncio.run(main())
