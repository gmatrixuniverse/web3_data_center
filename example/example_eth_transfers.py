import asyncio
import logging
from datetime import datetime
import sys
import os
from web3 import Web3
import math
from typing import List, Dict, Any

# Add the project root directory to the Python path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from web3_data_center.clients.opensearch_client import OpenSearchClient
from web3_data_center.clients.database.postgresql_client import PostgreSQLClient
from web3_data_center.utils.logger import get_logger

logger = get_logger(__name__)
logger.setLevel(logging.WARNING)

# Constants
START_BLOCK = 20048186
END_BLOCK = 20048189
BATCH_SIZE = 500  # Number of blocks per batch
PARALLEL_BATCHES = 5  # Number of parallel OpenSearch requests
DB_BATCH_SIZE = 1000  # Number of transfers per DB insert
QUERY_SIZE = 100  # Number of transactions per block
MIN_ETH = 0.0001  # Minimum ETH value to track

CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS eth_transfers (
    id SERIAL PRIMARY KEY,
    block_number INTEGER NOT NULL,
    block_timestamp TIMESTAMP NOT NULL,
    tx_hash VARCHAR(66) NOT NULL,
    from_address VARCHAR(42) NOT NULL,
    to_address VARCHAR(42) NOT NULL,
    value_wei VARCHAR(78) NOT NULL,
    value_eth NUMERIC(38,18) NOT NULL,
    token VARCHAR(42) NOT NULL,
    is_internal BOOLEAN NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
CREATE INDEX IF NOT EXISTS idx_eth_transfers_block_number ON eth_transfers(block_number);
CREATE INDEX IF NOT EXISTS idx_eth_transfers_from_address ON eth_transfers(from_address);
CREATE INDEX IF NOT EXISTS idx_eth_transfers_to_address ON eth_transfers(to_address);
"""

async def store_transfers_batch(pg_client: PostgreSQLClient, transfers: list):
    """Store ETH transfers in PostgreSQL database using batch insert"""
    if not transfers:
        return
        
    try:
        # Convert Wei to ETH for storage
        values = []
        for tx in transfers:
            value_wei = tx['Value']
            value_eth = int(value_wei) / 1e18
            if value_eth < MIN_ETH:
                continue
                
            values.append(
                f"({tx['BlockNumber']}, "
                f"'{tx['BlockTimestamp']}', "
                f"'{tx['Hash']}', "
                f"'{tx['FromAddress']}', "
                f"'{tx['ToAddress']}', "
                f"'{value_wei}', "
                f"{value_eth}, "
                f"'{tx['TokenAddress']}', "
                f"{tx.get('IsInternal', False)})"
            )

        if not values:
            return

        # Split values into smaller batches for better performance
        batch_size = DB_BATCH_SIZE
        for i in range(0, len(values), batch_size):
            batch_values = values[i:i + batch_size]
            
            # Batch insert with conflict handling
            insert_sql = f"""
                INSERT INTO eth_transfers (
                    block_number, block_timestamp, tx_hash, 
                    from_address, to_address, value_wei, value_eth, token, is_internal
                )
                VALUES {','.join(batch_values)};
            """
            
            await pg_client.execute(insert_sql)
            logger.warning(f"Stored {len(batch_values)} transfers in database")
        
    except Exception as e:
        logger.error(f"Error storing transfers: {str(e)}")
        raise

class ProgressTracker:
    def __init__(self, start_block: int, end_block: int, batch_size: int):
        self.start_block = start_block
        self.end_block = end_block
        self.batch_size = batch_size
        self.total_batches = math.ceil((end_block - start_block) / batch_size)
        self.processed_batches = 0
        self.total_transfers = 0
        self.start_time = datetime.now()
    
    def update(self, transfers_count: int = 0):
        """Update progress and log statistics"""
        self.processed_batches += 1
        self.total_transfers += transfers_count
        
        elapsed = (datetime.now() - self.start_time).total_seconds()
        blocks_processed = self.processed_batches * self.batch_size
        blocks_per_second = blocks_processed / elapsed if elapsed > 0 else 0
        
        logger.warning(
            f"Progress: {self.processed_batches}/{self.total_batches} batches | "
            f"Transfers: {self.total_transfers:,} | "
            f"Speed: {blocks_per_second:.0f} blocks/s"
        )

async def process_block_range(
    os_client: OpenSearchClient,
    pg_client: PostgreSQLClient,
    start_block: int,
    end_block: int,
    progress: ProgressTracker
) -> None:
    """Process a range of blocks in parallel batches"""
    # Calculate batch ranges
    num_blocks = end_block - start_block
    blocks_per_batch = BATCH_SIZE
    num_batches = math.ceil(num_blocks / blocks_per_batch)
    
    # Process batches in parallel groups
    for batch_group in range(0, num_batches, PARALLEL_BATCHES):
        batch_ranges = []
        for i in range(PARALLEL_BATCHES):
            batch_idx = batch_group + i
            if batch_idx >= num_batches:
                break
                
            batch_start = start_block + (batch_idx * blocks_per_batch)
            batch_end = min(batch_start + blocks_per_batch, end_block)
            batch_ranges.append({'start': batch_start, 'end': batch_end})
        
        # Get transfers for all batches in parallel
        min_value_wei = str(int(MIN_ETH * 1e18))
        transfers = await os_client.get_eth_transfers_batch(
            batch_ranges=batch_ranges,
            min_value=min_value_wei,
            size=QUERY_SIZE,
            max_parallel=PARALLEL_BATCHES
        )
        
        # Store transfers if any found
        if transfers:
            await store_transfers_batch(pg_client, transfers)
        
        # Update progress for completed batches
        for _ in range(len(batch_ranges)):
            progress.update(len(transfers))

async def main():
    """Main entry point"""
    # Initialize clients
    os_client = OpenSearchClient()
    pg_client = PostgreSQLClient(config_path='config.yml', db_section='zju')
    
    try:
        # Create table if not exists
        await pg_client.execute(CREATE_TABLE_SQL)
        
        # Initialize progress tracking
        progress = ProgressTracker(START_BLOCK, END_BLOCK, BATCH_SIZE * PARALLEL_BATCHES)
        
        # Process all blocks
        await process_block_range(
            os_client=os_client,
            pg_client=pg_client,
            start_block=START_BLOCK,
            end_block=END_BLOCK,
            progress=progress
        )
            
    except Exception as e:
        logger.error(f"Fatal error: {str(e)}")
        raise
    finally:
        await pg_client.close()
        await os_client.close()

async def query_specific_tx():
    """Query a specific transaction and its internal transactions."""
    os_client = OpenSearchClient()
    pg_client = PostgreSQLClient(config_path='config.yml', db_section='zju')
    
    # Create tables if they don't exist
    # await pg_client.execute(CREATE_TABLE_SQL)
    
    # Query specific block range
    tx_hash = "0xac6a71371254eda0d1fef868923b8fa92961b2f63353d2bfc5469dbcb44c6c30"
    block_number = 20048187  # The block number where this transaction exists
    
    transfers = await os_client.get_eth_transfers(
        start_block=block_number,
        end_block=block_number + 1,
        size=1
    )
    
    logger.warning(f"Found {len(transfers)} transfers for tx {tx_hash}")
    for t in transfers:
        if t["Hash"] == tx_hash:
            logger.warning(f"Transfer details: {t}")
    
    # Store transfers
    await store_transfers_batch(pg_client, transfers)
    
    # Close connections
    await os_client.close()
    await pg_client.close()

if __name__ == "__main__":
    # asyncio.run(main())  # Comment out the main function
    asyncio.run(query_specific_tx())
