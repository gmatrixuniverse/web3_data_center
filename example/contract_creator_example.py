import asyncio
import os
import sys
from typing import Optional
# Add the project root to Python path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from web3_data_center.core.data_center import DataCenter

async def lookup_contract_creator(data_center: DataCenter, contract_address: str) -> Optional[str]:
    """Look up creator address for a contract and print the result"""
    print(f"\nLooking up creator for contract: {contract_address}")
    creator = await data_center.get_contract_creator(contract_address)
    
    if creator:
        print(f"Creator address: {creator}")
        return creator
    else:
        print("No creator found")
        return None

async def main():
    # Initialize DataCenter with the project's config file
    config_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), "config.yml")
    
    async with DataCenter(config_path=config_path) as data_center:
        # Example contract addresses to look up
        contracts = [
            # Known contracts
            "0x00000000e88649dd6aab90088ca25d772d4607d0",  # Blur.io: Delegate Cash
            "0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2",  # WETH9
            "0x7a250d5630b4cf539739df2c5dacb4c659f2488d",  # Uniswap V2 Router
            "0x68b3465833fb72a70ecdf485e0e4c7bd8665fc45",  # Uniswap V3 Router
            
            # Invalid/Non-existent contracts
            "0xdead000000000000000042069420694206942069",
            "0x0000000000000000000000000000000000000000"
        ]

        results = {}
        for contract_address in contracts:
            creator = await lookup_contract_creator(data_center, contract_address)
            if creator:
                results[contract_address] = creator
        
        # Print summary
        print("\nSummary of contract creators:")
        print("-" * 50)
        for contract, creator in results.items():
            print(f"Contract: {contract}")
            print(f"Creator:  {creator}\n")

if __name__ == "__main__":
    asyncio.run(main())
