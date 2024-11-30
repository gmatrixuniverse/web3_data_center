import asyncio
import sys
import os
import json

# Add the project root directory to the Python path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from web3_data_center.clients.funding_client import FundingClient

async def main():
    # Initialize the client
    client = FundingClient(config_path="config.yml")

    try:
        # Test single address
        print("\n1. Getting first fund for a single address:")
        result = await client.simulate_view_first_fund(
            address="0x8d7523Ab01e19ecC1e08FaA31cE92b240814E41c"
        )
        print(json.dumps(result, indent=2))

        # Test batch addresses
        print("\n2. Getting first fund for multiple addresses:")
        addresses = [
            "0x8d7523Ab01e19ecC1e08FaA31cE92b240814E41c",
            "0xBE0eB53F46cd790Cd13851d5EFf43D12404d33E8"
        ]
        batch_results = await client.batch_simulate_view_first_fund(addresses)
        print(json.dumps(batch_results, indent=2))

    except Exception as e:
        print(f"Error occurred: {e}")

if __name__ == "__main__":
    asyncio.run(main())
