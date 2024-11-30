import sys
import os
import asyncio

# Add the project root to Python path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from web3_data_center.core.data_center import DataCenter

async def main():
    data_center = DataCenter()
    
    # Example developer address
    dev_address = "0x8d7523Ab01e19ecC1e08FaA31cE92b240814E41c"
    
    # Example target addresses to check
    target_addresses = [
        "0x5e809A85Aa182A9921EDD10a4163745bb3e36284",  # Address we checked before
        "0xf3D872b9E8d314820dc8E99DAfBe1A3FeEDc27D5",  # Another address
        "0xf3D872b9E8d314820dc8E99DAfBe1A3FeEDc27D5",  # Third address
        dev_address  # Include dev address itself to test filtering
    ]
    
    print(f"\nFinding funding relationships between dev address and targets:")
    print(f"Developer: {dev_address}")
    print(f"Targets: {', '.join(target_addresses)}")
    
    # First try without minimum depth requirement
    print("\n1. All funding relationships:")
    relationships = await data_center.find_dev_funding_relationships(
        dev_address,
        target_addresses
    )
    
    if relationships:
        print("\nFound relationships:")
        for target, common_funders in relationships.items():
            print(f"\nTarget: {target}")
            print(f"Number of common funders: {len(common_funders)}")
            for i, rel in enumerate(common_funders, 1):
                print(f"\nCommon Funder {i}:")
                print(f"└─ Address: {rel['common_funder']}")
                print(f"└─ Depth in dev path: {rel['dev_depth']}")
                print(f"└─ Depth in target path: {rel['target_depth']}")
                print(f"└─ Dev tx: {rel['dev_tx']}")
                print(f"└─ Target tx: {rel['target_tx']}")
    else:
        print("No relationships found")
    
    # Try again with minimum depth requirement
    min_depth = 3
    print(f"\n2. Relationships with minimum depth {min_depth}:")
    relationships = await data_center.find_dev_funding_relationships(
        dev_address,
        target_addresses,
        min_common_depth=min_depth
    )
    
    if relationships:
        print("\nFound relationships:")
        for target, common_funders in relationships.items():
            print(f"\nTarget: {target}")
            print(f"Number of common funders: {len(common_funders)}")
            for i, rel in enumerate(common_funders, 1):
                print(f"\nCommon Funder {i}:")
                print(f"└─ Address: {rel['common_funder']}")
                print(f"└─ Depth in dev path: {rel['dev_depth']}")
                print(f"└─ Depth in target path: {rel['target_depth']}")
                print(f"└─ Dev tx: {rel['dev_tx']}")
                print(f"└─ Target tx: {rel['target_tx']}")
    else:
        print("No relationships found with minimum depth requirement")

if __name__ == "__main__":
    asyncio.run(main())
