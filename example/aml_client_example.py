
import asyncio
import sys
import os


# Add the project root directory to the Python path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import asyncio
import json
from web3_data_center.clients.aml_client import AMLClient

async def main():
    # Initialize the client
    client = AMLClient(config_path="config.yml")

    try:
        # Get supported chains
        # print("\n1. Getting supported chains:")
        # chains = await client.get_supported_chains()
        # print(json.dumps(chains, indent=2))

        # Get labels for a single address (using Binance cold wallet as example)
        print("\n2. Getting labels for a single address:")
        address_labels = await client.get_address_labels(
            chain_id=1,  # Ethereum mainnet
            address="0xBE0eB53F46cd790Cd13851d5EFf43D12404d33E8"  # Binance cold wallet
        )
        print(json.dumps(address_labels, indent=2))

        # Get labels for multiple addresses
        # print("\n3. Getting labels for multiple addresses:")
        # addresses = [
        #     "0xBE0eB53F46cd790Cd13851d5EFf43D12404d33E8",  # Binance cold wallet
        #     "0x5e809A85Aa182A9921EDD10a4163745bb3e36284"   # Another example address
        # ]
        # batch_labels = await client.get_batch_address_labels(
        #     chain_id=1,
        #     addresses=addresses
        # )
        # print(json.dumps(batch_labels, indent=2))

        # Get entity info
        print("\n4. Getting entity info:")
        entity_info = await client.get_entity_info("BINANCE")
        print(json.dumps(entity_info, indent=2))

    except Exception as e:
        print(f"Error occurred: {e}")

if __name__ == "__main__":
    asyncio.run(main())
