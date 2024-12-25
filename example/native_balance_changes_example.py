import asyncio
import sys
import os
from pathlib import Path

# Add the parent directory to sys.path to import the package
parent_dir = str(Path(__file__).resolve().parent.parent)
if parent_dir not in sys.path:
    sys.path.append(parent_dir)

from web3_data_center.clients.opensearch_client import OpenSearchClient

async def main():
    # Example transaction hash that involves multiple balance changes
    tx_hash = "0x28ff51f1c84227a1d682e9ed75ee22b80920b4c18c6a860d1da2f57c17cdeac7"
    
    print(f"\nQuerying balance changes for transaction: {tx_hash}")
    print("-" * 80)
    
    async with OpenSearchClient() as client:
        try:
            balance_changes = await client.get_native_balance_changes(tx_hash)
            
            if not balance_changes:
                print("No balance changes found for this transaction.")
                return
                
            print(f"Found {len(balance_changes)} balance changes:")
            print("-" * 80)
            
            # Calculate total balance change
            total_change = 0
            
            for change in balance_changes:
                address = change['address']
                diff = change['difference']
                total_change += diff
                
                # Convert to ETH for better readability (assuming wei)
                diff_eth = diff / 1e18
                
                print(f"Address: {address}")
                print(f"Previous Balance (wei): {change['prev_balance']}")
                print(f"Current Balance (wei): {change['current_balance']}")
                print(f"Change: {diff_eth:,.18f} ETH")
                print("-" * 80)
            
            print(f"\nTotal Balance Change: {total_change / 1e18:,.18f} ETH")
            
        except Exception as e:
            print(f"Error: {str(e)}")

if __name__ == "__main__":
    asyncio.run(main())
