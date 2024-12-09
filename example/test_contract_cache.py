import asyncio
import time
import sys
import os
import shutil
from pathlib import Path


# Add the project root directory to the Python path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from web3_data_center import DataCenter, get_cache_dir

async def cleanup_cache():
    """Clean up the cache directory after tests"""
    cache_dir = get_cache_dir()
    if cache_dir.exists():
        shutil.rmtree(cache_dir)
        print(f"\nCleaned up cache directory: {cache_dir}")

async def test_contract_creator_cache():
    print("\n=== Testing Contract Creator Cache ===")
    
    try:
        # Initialize DataCenter
        async with DataCenter() as dc:
            # Test contract addresses (USDT and USDC contracts)
            contracts = [
                "0xdac17f958d2ee523a2206206994597c13d831ec7",  # USDT
                "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48",  # USDC
            ]
            
            for contract in contracts:
                print(f"\nTesting contract: {contract}")
                
                # Test 1: First call (should hit the database)
                print("\nTest 1: First call")
                start = time.time()
                result1 = await dc.get_contract_creator(contract)
                duration1 = time.time() - start
                print(f"First call duration: {duration1:.2f} seconds")
                print(f"Creator address: {result1}")
                
                # Test 2: Second call (should use cache)
                print("\nTest 2: Second call (should be faster)")
                start = time.time()
                result2 = await dc.get_contract_creator(contract)
                duration2 = time.time() - start
                print(f"Second call duration: {duration2:.2f} seconds")
                print(f"Cache hit? {'Yes' if duration2 < duration1 else 'No'}")
                print(f"Results match? {'Yes' if result1 == result2 else 'No'}")
            
            # Test 3: Invalid contract address
            print("\nTest 3: Invalid contract address")
            try:
                invalid_address = "0xinvalid"
                start = time.time()
                result = await dc.get_contract_creator(invalid_address)
                duration = time.time() - start
                print(f"Call duration: {duration:.2f} seconds")
                print(f"Result for invalid address: {result}")
            except Exception as e:
                print(f"Successfully caught error for invalid address: {e}")
            
            # Test 4: Non-contract address
            print("\nTest 4: Non-contract address")
            non_contract = "0x742d35Cc6634C0532925a3b844Bc454e4438f44e"  # A regular EOA
            start = time.time()
            result = await dc.get_contract_creator(non_contract)
            duration = time.time() - start
            print(f"Call duration: {duration:.2f} seconds")
            print(f"Result for non-contract address: {result}")

    except Exception as e:
        print(f"Error in contract creator test: {e}")

async def main():
    print("Starting cache tests...")
    print(f"Cache directory: {get_cache_dir()}")
    
    # Run tests
    await test_contract_creator_cache()
    
    # Clean up cache after tests
    await cleanup_cache()

if __name__ == "__main__":
    asyncio.run(main())
