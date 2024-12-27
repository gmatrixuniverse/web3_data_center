#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import sys
import time
import asyncio
import logging
from typing import List, Tuple
from statistics import mean, stdev

# Add the parent directory to the Python path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from web3_data_center.core.data_center import DataCenter

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

async def test_batch_performance(dc: DataCenter, batch_size: int, num_runs: int = 3) -> dict:
    """Test search_transaction_batch performance for a specific batch size."""
    # Get sample transactions
    tx_hashes = await dc.sample_transactions(block_range=(19000000, 21000000), sample_size=batch_size)
    # print(f"actual: {len(tx_hashes)}")
    # Run multiple times to get average performance
    times = []
    hit_counts = []
    
    for i in range(num_runs):
        start_time = time.time()
        print(len(tx_hashes))
        results = await dc.opensearch_client.search_transaction_batch(tx_hashes)
        end_time = time.time()
        
        duration = end_time - start_time
        hit_count = len(results['hits']['hits'])
        
        times.append(duration)
        hit_counts.append(hit_count)
        
        logger.info(f"Run {i+1}: {duration:.2f}s for {hit_count} hits")
        
    return {
        'batch_size': batch_size,
        'avg_time': mean(times),
        'std_time': stdev(times) if len(times) > 1 else 0,
        'avg_hits': mean(hit_counts),
        'times': times,
        'hits': hit_counts
    }

async def main():
    """Test search_transaction_batch with different batch sizes."""
    # Get config path
    config_path = os.path.join(
        os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
        'config.yml'
    )
    
    batch_sizes = [10, 100, 1000, 10000, 100000]
    results = []
    
    async with DataCenter(config_path=config_path) as dc:
        for size in batch_sizes:
            logger.info(f"\nTesting batch size: {size}")
            result = await test_batch_performance(dc, size)
            results.append(result)
            
            # Print results for this batch size
            print(f"\nResults for batch size {size}:")
            print(f"Average time: {result['avg_time']:.2f}s ± {result['std_time']:.2f}s")
            print(f"Average hits: {result['avg_hits']:.1f}")
            print(f"Individual runs:")
            for i, (t, h) in enumerate(zip(result['times'], result['hits']), 1):
                print(f"  Run {i}: {t:.2f}s, {h} hits")
            
            # Small delay between tests
            await asyncio.sleep(1)
        
        # Print summary
        print("\nPerformance Summary:")
        print("Batch Size | Avg Time (s) | Std Dev (s) | Avg Hits")
        print("-" * 50)
        for r in results:
            print(f"{r['batch_size']:^10} | {r['avg_time']:^11.2f} | {r['std_time']:^10.2f} | {r['avg_hits']:^8.1f}")

if __name__ == '__main__':
    asyncio.run(main())