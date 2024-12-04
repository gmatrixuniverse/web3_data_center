import sys
import os
import asyncio
from pathlib import Path

# Add the project root to Python path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from web3_data_center.core.data_center import DataCenter

async def print_funding_path(path):
    for step in path:
        print(f"Depth {step['depth']}:")
        print(f"└─ Funder: {step['address']}")
        print(f"└─ Label: {step['label']}")
        print(f"└─ Entity: {step['entity']}")
        if step['name_tag']:
            print(f"└─ Name: {step['name_tag']}")
        if step['type']:
            print(f"└─ Type: {step['type']}")
        print(f"└─ Transaction: {step['tx_hash']}")

def get_relationship_type(common_type: int) -> str:
    if common_type == 1:
        return "Same Entity"
    elif common_type == 2:
        return "Same Address (No Entity)"
    else:
        return "Unknown"

async def main():
    data_center = DataCenter()

    # Example addresses to check relationship between
    dev = "0x5eD3744745D6A27C79a6EC3b3e76cd4e68Ad494B"
    address1 = "0xd0Ffcf98A5E912d746e67FbA6911cB6D437ea7f1"
    address2 = "0xdF4964c8EC8f84b9A9Be1F12196b757076986412"
    
    # print("\nFunding path for Address 1:")
    # path1 = await data_center.get_funding_path(address1)
    # await print_funding_path(path1)
    
    # print("\nFunding path for Address 2:")
    # path2 = await data_center.get_funding_path(address2)
    # await print_funding_path(path2)
    
    print("\nChecking funding relationship...")
    relationship = await data_center.check_funding_relationship(dev, address1)
    
    if relationship:
        print("\nFound funding relationship!")
        print(f"Common Funder 1: {relationship['common_funder1']}")
        if relationship['common_funder1'] != relationship['common_funder2']:
            print(f"Common Funder 2: {relationship['common_funder2']}")
        print(f"Relationship Type: {get_relationship_type(relationship['common_type'])}")
        print(f"Entity: {relationship['entity']}")
        print(f"Label: {relationship['label']}")
        if relationship['name_tag']:
            print(f"Name: {relationship['name_tag']}")
        print(f"Depth in Path 1: {relationship['depth1']}")
        print(f"Depth in Path 2: {relationship['depth2']}")
        print(f"Transaction 1: {relationship['tx_hash1']}")
        print(f"Transaction 2: {relationship['tx_hash2']}")
    else:
        print("\nNo funding relationship found")

    relationship = await data_center.check_funding_relationship(dev, address2)
    if relationship:
        print("\nFound funding relationship!")
        print(f"Common Funder 1: {relationship['common_funder1']}")
        if relationship['common_funder1'] != relationship['common_funder2']:
            print(f"Common Funder 2: {relationship['common_funder2']}")
        print(f"Relationship Type: {get_relationship_type(relationship['common_type'])}")
        print(f"Entity: {relationship['entity']}")
        print(f"Label: {relationship['label']}")
        if relationship['name_tag']:
            print(f"Name: {relationship['name_tag']}")
        print(f"Depth in Path 1: {relationship['depth1']}")
        print(f"Depth in Path 2: {relationship['depth2']}")
        print(f"Transaction 1: {relationship['tx_hash1']}")
        print(f"Transaction 2: {relationship['tx_hash2']}")
    else:
        print("\nNo funding relationship found")

if __name__ == "__main__":
    asyncio.run(main())
