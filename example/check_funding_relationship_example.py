import sys
import os
import asyncio
from pathlib import Path

# Add the project root to Python path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from web3_data_center.core.data_center import DataCenter

async def print_funding_path(data_center, address, path_number):
    print(f"\n{path_number}. Funding path for Address {path_number}:")
    path = await data_center.get_funding_path(address)
    for step in path:
        print(f"Depth {step['depth']}:")
        print(f"└─ Funder: {step['address']}")
        print(f"└─ Label: {step['label']}")
        if step['name_tag']:
            print(f"└─ Name: {step['name_tag']}")
        if step['type']:
            print(f"└─ Type: {step['type']}")
        print(f"└─ Transaction: {step['tx_hash']}")

async def main():
    data_center = DataCenter()

    # Example addresses to check relationship between
    address1 = "0x6BFB20E1Db67578e275FDC3FF0E7d081c4B22a8D"
    address2 = "0x114b07B99Aaeec2a9114314e1AB85ae9Ffd294ed"
    
    # print(f"\nChecking funding relationship between:")
    # print(f"Address 1: {address1}")
    # print(f"Address 2: {address2}")
    
    # First, let's see the funding paths for both addresses
    await print_funding_path(data_center, address1, 1)
    await print_funding_path(data_center, address2, 2)
    
    # Now check if they have a funding relationship
    print("\n3. Checking for common funders:")
    result = await data_center.check_funding_relationship(address1, address2)
    if result:
        print("✓ Funding relationship found!")
        print(f"└─ Common funder: {result['common_funder']}")
        print(f"└─ Label: {result['label']}")
        if result['name_tag']:
            print(f"└─ Name: {result['name_tag']}")
        if result['type']:
            print(f"└─ Type: {result['type']}")
        print(f"└─ Depth in path 1: {result['depth1']}")
        print(f"└─ Depth in path 2: {result['depth2']}")
        print(f"└─ Transaction 1: {result['tx_hash1']}")
        print(f"└─ Transaction 2: {result['tx_hash2']}")
    else:
        print("✗ No funding relationship found")

if __name__ == "__main__":
    asyncio.run(main())
