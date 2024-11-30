import sys
import os
import asyncio
from pathlib import Path

# Add the project root to Python path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from web3_data_center.core.data_center import DataCenter

async def main():
    data_center = DataCenter()
    
    # Example addresses to check relationship between
    address1 = "0x8d7523Ab01e19ecC1e08FaA31cE92b240814E41c"
    address2 = "0x5e809A85Aa182A9921EDD10a4163745bb3e36284"
    
    print(f"\nChecking funding relationship between:")
    print(f"Address 1: {address1}")
    print(f"Address 2: {address2}")
    
    # First, let's see the funding paths for both addresses
    print("\n1. Funding path for Address 1:")
    path1 = await data_center.get_funding_path(address1)
    for step in path1:
        print(f"Depth {step['depth']}:")
        print(f"└─ Funder: {step['address']}")
        print(f"└─ Transaction: {step['tx_hash']}")
    
    print("\n2. Funding path for Address 2:")
    path2 = await data_center.get_funding_path(address2)
    for step in path2:
        print(f"Depth {step['depth']}:")
        print(f"└─ Funder: {step['address']}")
        print(f"└─ Transaction: {step['tx_hash']}")
    
    # Now check if they have a funding relationship
    print("\n3. Checking for common funders:")
    relationship = await data_center.check_funding_relationship(address1, address2)
    
    if relationship:
        print("✓ Funding relationship found!")
        print(f"└─ Common funder: {relationship['common_funder']}")
        print(f"└─ Depth in path 1: {relationship['depth1']}")
        print(f"└─ Depth in path 2: {relationship['depth2']}")
        print(f"└─ Transaction 1: {relationship['tx_hash1']}")
        print(f"└─ Transaction 2: {relationship['tx_hash2']}")
    else:
        print("✗ No funding relationship found")

if __name__ == "__main__":
    asyncio.run(main())
