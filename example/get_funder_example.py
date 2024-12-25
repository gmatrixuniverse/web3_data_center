import sys
import asyncio
from pathlib import Path

# Add the project root to Python path
project_root = str(Path(__file__).parent.parent)
sys.path.insert(0, project_root)

from web3_data_center.core.data_center import DataCenter

async def main():
    async with DataCenter() as data_center:
        # Example address
        address = "0x8d7523Ab01e19ecC1e08FaA31cE92b240814E41c"
        
        print("\n1. Getting funding tree (up to 4 levels):")
        tree = await data_center.get_funder_tree([address], max_depth=4)
        if tree and tree[address]:
            print(f"Funding tree for {address}:")
            def print_tree(node, indent=""):
                if not node or isinstance(node, str):
                    return
                print(f"{indent}└─ Funder: {node['funder']}")
                print(f"{indent}   Transaction: {node['funded_at']}")
                if node.get('next_level'):
                    print_tree(node['next_level'], indent + "   ")
            print_tree(tree[address])
        else:
            print("No funding information found")

        print("\n2. Getting root funder (searching as deep as possible):")
        root = await data_center.get_root_funder(address)
        if root:
            print(f"Root funder found at depth {root['depth']}:")
            print(f"└─ Address: {root['address']}")
            print(f"└─ Last funding tx: {root['tx_hash']}")
            print(f"└─ Is confirmed root: {root['is_root']}")
        else:
            print("No root funder found")

        print("\n3. Getting root funders for multiple addresses:")
        addresses = [
            address,
            "0x5e809A85Aa182A9921EDD10a4163745bb3e36284",
            "0xf3D872b9E8d314820dc8E99DAfBe1A3FeEDc27D5"
        ]
        roots = await data_center.get_root_funders(addresses)
        for addr, root in roots.items():
            print(f"\nAddress: {addr}")
            if root:
                print(f"└─ Root funder found at depth {root['depth']}:")
                print(f"  └─ Address: {root['address']}")
                print(f"  └─ Last funding tx: {root['tx_hash']}")
                print(f"  └─ Is confirmed root: {root['is_root']}")
            else:
                print("└─ No root funder found")

if __name__ == "__main__":
    asyncio.run(main())
