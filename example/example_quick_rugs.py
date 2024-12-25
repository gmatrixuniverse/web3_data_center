import asyncio
import logging
import sys
import os
from typing import List, Dict, Any

# Add the project root directory to the Python path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from web3_data_center.clients.database.postgresql_client import PostgreSQLClient
from web3_data_center.utils.logger import get_logger

logger = get_logger(__name__)
logger.setLevel(logging.INFO)

async def get_quick_removal_pairs(pg_client: PostgreSQLClient, block_threshold: int = 30) -> List[Dict[str, Any]]:
    """
    Get pairs where liquidity was removed within a specified number of blocks after addition.
    
    Args:
        pg_client: PostgreSQL client instance
        block_threshold: Maximum number of blocks between add and remove (default: 30)
    
    Returns:
        List of pairs with quick liquidity removal
    """
    query = """
    SELECT p.*, a.first_add_block, r.first_remove_block
    FROM (
        SELECT 
            l.address,
            MIN(l.block) AS first_add_block
        FROM eth_lps l
        WHERE (l.amount0::numeric > 0 OR l.amount1::numeric > 0)
        GROUP BY l.address
    ) a
    JOIN (
        SELECT 
            l.address,
            MIN(l.block) AS first_remove_block
        FROM eth_lps l
        WHERE (l.amount0::numeric < 0 OR l.amount1::numeric < 0)
        GROUP BY l.address
    ) r ON a.address = r.address
    JOIN eth_pairs p ON p.address = a.address
    WHERE (r.first_remove_block - a.first_add_block) < %(threshold)s
    AND (r.first_remove_block - a.first_add_block) > 0;
    """
    
    return await pg_client.execute(query, {'threshold': block_threshold})

async def get_quote_tokens(pg_client: PostgreSQLClient) -> Dict[str, int]:
    """
    Get quote tokens and their priorities.
    
    Args:
        pg_client: PostgreSQL client instance
    
    Returns:
        Dictionary mapping token addresses to their priorities
    """
    query = "SELECT token_address, priority FROM quote_priority ORDER BY priority;"
    results = await pg_client.execute(query)
    return {row['token_address']: row['priority'] for row in results}

async def get_swap_count(pg_client: PostgreSQLClient, pair_address: str) -> int:
    """
    Get the number of swap records for a pair.
    
    Args:
        pg_client: PostgreSQL client instance
        pair_address: Address of the trading pair
    
    Returns:
        Number of swap records
    """
    query = """
    SELECT COUNT(*) as swap_count
    FROM eth_swaps
    WHERE address = %(address)s;
    """
    
    result = await pg_client.execute(query, {'address': pair_address})
    return result[0]['swap_count'] if result else 0

async def analyze_liquidity_changes(
    pg_client: PostgreSQLClient,
    pair_address: str,
    quote_token: str,
    first_add_block: int,
    first_remove_block: int
) -> Dict[str, Any]:
    """
    Analyze liquidity changes for a specific pair.
    
    Args:
        pg_client: PostgreSQL client instance
        pair_address: Address of the trading pair
        quote_token: Address of the quote token
        first_add_block: Block number of first liquidity addition
        first_remove_block: Block number of first liquidity removal
    
    Returns:
        Dictionary containing liquidity analysis results
    """
    query = """
    SELECT 
        CASE 
            WHEN token0 = %(quote_token)s THEN amount0::numeric
            WHEN token1 = %(quote_token)s THEN amount1::numeric
        END as quote_amount,
        block
    FROM eth_lps l
    JOIN eth_pairs p ON l.address = p.address
    WHERE l.address = %(pair_address)s 
    AND l.block IN (%(add_block)s, %(remove_block)s);
    """
    
    results = await pg_client.execute(
        query, 
        {
            'pair_address': pair_address,
            'quote_token': quote_token,
            'add_block': first_add_block,
            'remove_block': first_remove_block
        }
    )
    
    # Process results
    initial_amount = next((r['quote_amount'] for r in results if r['block'] == first_add_block), 0)
    removed_amount = next((r['quote_amount'] for r in results if r['block'] == first_remove_block), 0)
    
    if initial_amount == 0:
        removal_percentage = 0
    else:
        removal_percentage = abs(removed_amount / initial_amount * 100)
    
    return {
        'pair_address': pair_address,
        'quote_token': quote_token,
        'initial_amount': initial_amount,
        'removed_amount': removed_amount,
        'removal_percentage': removal_percentage,
        'blocks_to_remove': first_remove_block - first_add_block
    }

async def find_suspicious_pairs(
    pg_client: PostgreSQLClient,
    block_threshold: int = 30,
    removal_threshold: float = 90,
    max_swaps: int = 15000
) -> List[Dict[str, Any]]:
    """
    Find pairs with suspicious liquidity removal patterns.
    
    Args:
        pg_client: PostgreSQL client instance
        block_threshold: Maximum blocks between add and remove (default: 30)
        removal_threshold: Minimum percentage of quote token removed (default: 90)
        max_swaps: Maximum number of swap records allowed (default: 15000)
    
    Returns:
        List of suspicious pairs with their analysis
    """
    logger.info(f"Finding pairs with liquidity removed within {block_threshold} blocks...")
    
    # Get quick removal pairs
    quick_pairs = await get_quick_removal_pairs(pg_client, block_threshold)
    if not quick_pairs:
        logger.info("No pairs found with quick liquidity removal")
        return []
    
    logger.info(f"Found {len(quick_pairs)} pairs with quick liquidity removal")
    
    # Get quote tokens
    logger.info("Fetching quote tokens...")
    quote_tokens = await get_quote_tokens(pg_client)
    if not quote_tokens:
        logger.error("No quote tokens found")
        return []
    
    logger.info(f"Found {len(quote_tokens)} quote tokens")
    suspicious_pairs = []
    
    logger.info("Analyzing liquidity changes for each pair...")
    for i, pair in enumerate(quick_pairs, 1):
        if i % 100 == 0:  # Log progress every 100 pairs
            logger.info(f"Progress: Analyzing pair {i}/{len(quick_pairs)}")
            
        # Check swap count
        swap_count = await get_swap_count(pg_client, pair['address'])
        if swap_count > max_swaps:
            logger.debug(f"Skipping pair {pair['address']}: too many swaps ({swap_count} > {max_swaps})")
            continue
            
        # Find the highest priority quote token in the pair
        pair_quote_token = None
        highest_priority = float('inf')
        
        for token_addr, priority in quote_tokens.items():
            if token_addr in (pair['token0'], pair['token1']) and priority < highest_priority:
                pair_quote_token = token_addr
                highest_priority = priority
        
        if not pair_quote_token:
            continue
        
        # Analyze liquidity changes
        try:
            analysis = await analyze_liquidity_changes(
                pg_client,
                pair['address'],
                pair_quote_token,
                pair['first_add_block'],
                pair['first_remove_block']
            )
            
            if analysis['removal_percentage'] >= removal_threshold:
                analysis['swap_count'] = swap_count  # Add swap count to the analysis
                suspicious_pairs.append({
                    **pair,
                    **analysis,
                    'quote_token_priority': highest_priority
                })
                logger.info(
                    f"Found suspicious pair: {pair['address']} "
                    f"(Removed: {analysis['removal_percentage']:.1f}% in {analysis['blocks_to_remove']} blocks, "
                    f"Swaps: {swap_count})"
                )
        except Exception as e:
            logger.error(f"Error analyzing pair {pair['address']}: {str(e)}")
            continue
    
    logger.info(f"\nAnalysis complete. Found {len(suspicious_pairs)} suspicious pairs")
    return sorted(
        suspicious_pairs,
        key=lambda x: (x['blocks_to_remove'], -x['removal_percentage'])
    )

async def save_suspicious_pairs(pg_client: PostgreSQLClient, pairs: List[Dict[str, Any]]) -> None:
    """
    Save suspicious pairs to database.
    
    Args:
        pg_client: PostgreSQL client instance
        pairs: List of suspicious pairs with their analysis
    """
    if not pairs:
        logger.info("No pairs to save")
        return

    # Create table if not exists
    create_table_query = """
    CREATE TABLE IF NOT EXISTS quick_removal_pairs (
        pair_address VARCHAR(42) PRIMARY KEY,
        token0 VARCHAR(42),
        token1 VARCHAR(42),
        quote_token VARCHAR(42),
        first_add_block BIGINT,
        first_remove_block BIGINT,
        blocks_to_remove INTEGER,
        initial_amount NUMERIC,
        removed_amount NUMERIC,
        removal_percentage NUMERIC,
        quote_token_priority INTEGER,
        swap_count INTEGER,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    """
    
    await pg_client.execute(create_table_query)
    logger.info("Table 'quick_removal_pairs' created or already exists")

    # Prepare insert query
    insert_query = """
    INSERT INTO quick_removal_pairs (
        pair_address, token0, token1, quote_token, 
        first_add_block, first_remove_block, blocks_to_remove,
        initial_amount, removed_amount, removal_percentage, 
        quote_token_priority, swap_count
    ) VALUES (
        %(pair_address)s, %(token0)s, %(token1)s, %(quote_token)s,
        %(first_add_block)s, %(first_remove_block)s, %(blocks_to_remove)s,
        %(initial_amount)s, %(removed_amount)s, %(removal_percentage)s,
        %(quote_token_priority)s, %(swap_count)s
    )
    ON CONFLICT (pair_address) DO UPDATE SET
        first_add_block = EXCLUDED.first_add_block,
        first_remove_block = EXCLUDED.first_remove_block,
        blocks_to_remove = EXCLUDED.blocks_to_remove,
        initial_amount = EXCLUDED.initial_amount,
        removed_amount = EXCLUDED.removed_amount,
        removal_percentage = EXCLUDED.removal_percentage,
        quote_token_priority = EXCLUDED.quote_token_priority,
        swap_count = EXCLUDED.swap_count,
        created_at = CURRENT_TIMESTAMP;
    """

    # Insert pairs in batches
    batch_size = 1000
    total_pairs = len(pairs)
    
    for i in range(0, total_pairs, batch_size):
        batch = pairs[i:i + batch_size]
        try:
            # Convert batch data to list of parameter dictionaries
            batch_params = [{
                'pair_address': pair['address'],
                'token0': pair['token0'],
                'token1': pair['token1'],
                'quote_token': pair['quote_token'],
                'first_add_block': pair['first_add_block'],
                'first_remove_block': pair['first_remove_block'],
                'blocks_to_remove': pair['blocks_to_remove'],
                'initial_amount': str(pair['initial_amount']),
                'removed_amount': str(pair['removed_amount']),
                'removal_percentage': str(pair['removal_percentage']),
                'quote_token_priority': pair['quote_token_priority'],
                'swap_count': pair['swap_count']
            } for pair in batch]
            
            # Execute batch insert using execute_batch
            pg_client.execute_batch(insert_query, batch_params)
            logger.info(f"Saved pairs {i + 1} to {min(i + batch_size, total_pairs)} of {total_pairs}")
        except Exception as e:
            logger.error(f"Error saving batch starting at index {i}: {str(e)}")
            raise

    logger.info(f"Successfully saved {total_pairs} pairs to database")

async def main():
    """Example usage of the quick removal analysis."""
    config_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'config.yml')
    
    try:
        # Initialize PostgreSQL client
        logger.info("Connecting to database...")
        pg_client = PostgreSQLClient(config_path=config_path, db_section='zju')
        
        # Find suspicious pairs
        logger.info("\nStarting analysis of quick liquidity removals...")
        suspicious_pairs = await find_suspicious_pairs(
            pg_client,
            block_threshold=30,
            removal_threshold=90
        )
        
        # Save results to database
        logger.info("\nSaving results to database...")
        await save_suspicious_pairs(pg_client, suspicious_pairs)
        
        # Print results
        logger.info("\nResults:")
        print(f"\nFound {len(suspicious_pairs)} suspicious pairs:")
        for pair in suspicious_pairs:
            print(f"\nPair Address: {pair['address']}")
            print(f"Non-quote Token: {pair['token1'] if pair['token0'] == pair['quote_token'] else pair['token0']}")
            print(f"Quote Token: {pair['quote_token']}")
            print(f"Blocks to Remove: {pair['blocks_to_remove']}")
            print(f"Removal Percentage: {pair['removal_percentage']:.2f}%")
            print(f"Initial Amount: {pair['initial_amount']}")
            print(f"Removed Amount: {pair['removed_amount']}")
            
    except Exception as e:
        logger.error(f"Error in main: {e}")
        raise
    finally:
        await pg_client.close()
        logger.info("Analysis complete. Database connection closed.")

if __name__ == "__main__":
    asyncio.run(main())
