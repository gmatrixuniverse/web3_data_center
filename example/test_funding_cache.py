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

async def test_funding_path_cache():
    print("\n=== Testing Funding Path Cache ===")
    
    try:
        # Initialize DataCenter
        async with DataCenter() as dc:
            # Test address
            address = "0x7dcf4527bdf7503b156f7824b0fc6b11304ed995"
            
            # Test 1: First call (should hit the database)
            print("\nTest 1: First call")
            start = time.time()
            result1 = await dc.get_funding_path(address)
            duration1 = time.time() - start
            print(f"First call duration: {duration1:.2f} seconds")
            print(f"Path length: {len(result1) if result1 else 0}")
            if result1:
                print("First few funders:")
                for i, step in enumerate(result1[:3]):
                    print(f"  {i+1}. {step['address']} (depth: {step['depth']})")
            
            # Test 2: Second call (should use cache)
            print("\nTest 2: Second call (should be faster)")
            start = time.time()
            result2 = await dc.get_funding_path(address)
            duration2 = time.time() - start
            print(f"Second call duration: {duration2:.2f} seconds")
            print(f"Cache hit? {'Yes' if duration2 < duration1 else 'No'}")
            
            # Test 3: Different max_depth (should hit database)
            print("\nTest 3: Different max_depth")
            start = time.time()
            result3 = await dc.get_funding_path(address, max_depth=5)
            duration3 = time.time() - start
            print(f"Different max_depth duration: {duration3:.2f} seconds")
            print(f"Path length: {len(result3) if result3 else 0}")
            
            # Test 4: Different address (should hit database)
            print("\nTest 4: Different address")
            different_address = "0x04bda42de3bc32abb00df46004204424d4cf8287"
            start = time.time()
            result4 = await dc.get_funding_path(different_address)
            duration4 = time.time() - start
            print(f"Different address duration: {duration4:.2f} seconds")
            print(f"Path length: {len(result4) if result4 else 0}")
            
            # Test 5: Error handling (invalid address)
            print("\nTest 5: Invalid address handling")
            try:
                await dc.get_funding_path("0xinvalid")
                print("Error: Should have raised an exception for invalid address")
            except ValueError as e:
                print(f"Successfully caught invalid address error: {e}")
            
    except Exception as e:
        print(f"Error in funding path test: {e}")

async def test_funding_relationship_cache():
    print("\n=== Testing Funding Relationship Cache ===")
    
    try:
        # Initialize DataCenter
        async with DataCenter() as dc:
            # Test addresses
            address1 = "0x3dA747C9c46fcb81e5b049FC1722D83455B4a92a"
            address2 = "0x889d1e4e35deCDaa83C3593e394169AF8cD95750"
            
            # Test 1: First call (should hit the database)
            print("\nTest 1: First call")
            start = time.time()
            result1 = await dc.check_funding_relationship(address1, address2)
            print(result1)
            duration1 = time.time() - start
            print(f"First call duration: {duration1:.2f} seconds")
            print(f"Result: {result1}")
            
            # Test 2: Second call (should use cache)
            print("\nTest 2: Second call (should be faster)")
            start = time.time()
            result2 = await dc.check_funding_relationship(address1, address2)
            duration2 = time.time() - start
            print(result2)
            print(f"Second call duration: {duration2:.2f} seconds")
            print(f"Cache hit? {'Yes' if duration2 < duration1 else 'No'}")
            
            # Test 3: Different order of addresses (should still use cache)
            print("\nTest 3: Different order of addresses")
            start = time.time()
            result3 = await dc.check_funding_relationship(address2, address1)
            duration3 = time.time() - start
            print(f"Different order duration: {duration3:.2f} seconds")
            print(f"Cache hit? {'Yes' if duration3 < duration1 else 'No'}")
            
            # Test 4: Different max_depth (should hit database)
            print("\nTest 4: Different max_depth")
            start = time.time()
            result4 = await dc.check_funding_relationship(address1, address2, max_depth=5)
            duration4 = time.time() - start
            print(f"Different max_depth duration: {duration4:.2f} seconds")
            print(f"Result: {result4}")
            
            # Test 5: Error handling (invalid address)
            print("\nTest 5: Invalid address handling")
            try:
                await dc.check_funding_relationship("0xinvalid", address2)
                print("Error: Should have raised an exception for invalid address")
            except ValueError as e:
                print(f"Successfully caught invalid address error: {e}")
            
    except Exception as e:
        print(f"Error in funding relationship test: {e}")

async def main():
    print("Starting cache tests...")
    print(f"Cache directory: {get_cache_dir()}")
    
    # Run tests
    # await test_funding_path_cache()
    await test_funding_relationship_cache()
    
    # Clean up cache after tests
    await cleanup_cache()

if __name__ == "__main__":
    asyncio.run(main())
