import sys
import asyncio
from pathlib import Path
from datetime import datetime

# Add the project root to Python path
project_root = str(Path(__file__).parent.parent)
sys.path.insert(0, project_root)

from web3_data_center.core.data_center import DataCenter

async def main():
    print(f"\n[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] Starting funder analysis...")
    async with DataCenter() as data_center:
        # Example address
        address = "0x1a1aa05b9aA0a78b35257f810Ca06Af4cBaa9466"
        
        print(f"\n[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] 1. Getting funding path (max depth: 30 levels)")
        print("=" * 50)
        path = await data_center.get_funding_path(address, max_depth=30)
        if path:
            print(f"📊 Funding path for address:\n🔷 {address}")
            for step in path:
                depth = step['depth']
                indent = "   " * (depth - 1)
                print(f"{indent}└─ 👤 Funder: {step['address']}")
                print(f"{indent}   📅 Transaction: {step['tx_hash']}")
                if step.get('is_cex'):
                    print(f"{indent}   🏦 CEX/Exchange detected")
                if step.get('label') and step['label'] != 'Default':
                    print(f"{indent}   🏷️  Label: {step['label']}")
                if step.get('name_tag'):
                    print(f"{indent}   📝 Name: {step['name_tag']}")
                if step.get('type') and step['type'] != 'DEFAULT':
                    print(f"{indent}   🔹 Type: {step['type']}")
                if step.get('entity'):
                    print(f"{indent}   🏢 Entity: {step['entity']}")
        else:
            print("❌ No funding information found")

        print(f"\n[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] 2. Getting root funder")
        print("=" * 50)
        root = await data_center.get_root_funder(address)
        if root:
            print(f"🎯 Root funder details (found at depth {root['depth']}):")
            print(f"└─ 📍 Address: {root['address']}")
            print(f"└─ 🔗 Transaction Hash: {root['tx_hash']}")
            print(f"└─ ✅ Root Status: {'Confirmed root' if root['is_root'] else 'Potential root'}")
            print(f"└─ 📏 Depth: {root['depth']} levels from target")
        else:
            print("❌ No root funder found")

        print(f"\n[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] 3. Analyzing multiple addresses")
        print("=" * 50)
        addresses = [
            address,
            "0x5e809A85Aa182A9921EDD10a4163745bb3e36284",
            "0xf3D872b9E8d314820dc8E99DAfBe1A3FeEDc27D5"
        ]
        print(f"📋 Processing {len(addresses)} addresses...")
        roots = await data_center.get_root_funders(addresses)
        for addr, root in roots.items():
            print(f"\n🔍 Analysis for: {addr}")
            if root:
                print(f"└─ 🎯 Root funder details:")
                print(f"   └─ 📍 Address: {root['address']}")
                print(f"   └─ 🔗 Transaction: {root['tx_hash']}")
                print(f"   └─ ✅ Status: {'Confirmed root' if root['is_root'] else 'Potential root'}")
                print(f"   └─ 📏 Depth: {root['depth']} levels from target")
            else:
                print("└─ ❌ No root funder found")

if __name__ == "__main__":
    asyncio.run(main())
