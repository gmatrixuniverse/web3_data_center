import asyncio
from web3_data_center.core.data_center import DataCenter

async def main():
    data_center = DataCenter()

    # Example address to find root funder for
    address = "0x8d7523Ab01e19ecC1e08FaA31cE92b240814E41c"
    print(f"\nGetting root funder for single address: {address}")
    result = await data_center.get_root_funder(address)
    if result:
        print(f"Root funder found at depth {result['depth']}:")
        print(f"└─ Address: {result['address']}")
        print(f"└─ Last funding tx: {result['tx_hash']}")
        print(f"└─ Is confirmed root: {result['is_root']}")
    else:
        print("No funding information found")

    # Example addresses for batch root funder lookup
    addresses = [
        "0x8d7523Ab01e19ecC1e08FaA31cE92b240814E41c",
        "0x5e809A85Aa182A9921EDD10a4163745bb3e36284",
        "0xf3D872b9E8d314820dc8E99DAfBe1A3FeEDc27D5"
    ]
    print("\nGetting root funders for multiple addresses:\n")
    results = await data_center.get_root_funders(addresses)
    
    for addr in addresses:
        print(f"Address: {addr}")
        result = results[addr]
        if result:
            print(f"└─ Root funder found at depth {result['depth']}:")
            print(f"  └─ Address: {result['address']}")
            print(f"  └─ Last funding tx: {result['tx_hash']}")
            print(f"  └─ Is confirmed root: {result['is_root']}")
        else:
            print("└─ No funding information found")
        print()

if __name__ == "__main__":
    asyncio.run(main())
