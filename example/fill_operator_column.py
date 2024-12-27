#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import sys
import asyncio
import logging
from typing import List, Dict, Any
from tqdm import tqdm

# Add the parent directory to the Python path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from web3_data_center.core.data_center import DataCenter

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

async def update_operator_batch(dc: DataCenter, batch: List[Dict[str, Any]], pbar: tqdm):
    """Update operators for a batch of transactions."""
    try:
        # Get transaction details from OpenSearch
        tx_hashes = [row['txhash'] for row in batch]
        
        tx_results = await dc.opensearch_client.search_transaction_batch(tx_hashes)
        
        updated_count = 0
        # Process each transaction
        for hit in tx_results['hits']['hits']:
            for tx_hit in hit['inner_hits']['Transactions']['hits']['hits']:
                tx = tx_hit['_source']
                tx_hash = tx['Hash']
                
                # Find matching row from our batch
                matching_row = next((row for row in batch if row['txhash'].lower() == tx_hash.lower()), None)
                if not matching_row:
                    continue
                
                # Update operator in database
                update_sql = """
                UPDATE eth_lps_with_operator
                SET operator = %s
                WHERE id = %s AND operator IS NULL
                """
                dc.postgres_client.execute_dml(
                    update_sql,
                    [tx.get('FromAddress', '').lower(), matching_row['id']]
                )
                updated_count += 1
                logger.debug(f"Updated operator for tx {tx_hash}")
        
        # Update progress bar
        pbar.update(len(batch))
        pbar.set_postfix({"Updated": updated_count})
                
    except Exception as e:
        logger.error(f"Error updating batch: {str(e)}")

async def fill_operator_column():
    """Fill operator column for transactions where it's missing."""
    # Get config path
    config_path = os.path.join(
        os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
        'config.yml'
    )
    
    async with DataCenter(config_path=config_path) as dc:
        # First, get total count of rows to process
        count_query = """
        SELECT COUNT(*) as count
        FROM eth_lps_with_operator
        WHERE operator IS NULL
        """
        result = dc.postgres_client.execute_query(count_query)
        total_rows = result[0]['count']
        
        if total_rows == 0:
            logger.info("No rows need processing")
            return
            
        logger.info(f"Found {total_rows} rows to process")
        
        # Create progress bar
        with tqdm(total=total_rows, desc="Processing transactions", unit="tx") as pbar:
            while True:  # Keep processing until no more rows to update
                # Query rows that need operator
                query = """
                SELECT id, txhash
                FROM eth_lps_with_operator
                WHERE operator IS NULL
                LIMIT 10000  -- Process in batches
                """
                rows = dc.postgres_client.execute_query(query)
                
                if not rows:
                    break
                
                # Update operators for this batch
                await update_operator_batch(dc, rows, pbar)
                
                # Small delay to prevent overloading
                await asyncio.sleep(1)
        
        logger.info("Processing complete!")

if __name__ == '__main__':
    asyncio.run(fill_operator_column())
