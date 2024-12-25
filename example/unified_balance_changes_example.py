import asyncio
import sys
import os
from pathlib import Path
from typing import Dict, Any

# Add the parent directory to sys.path to import the package
parent_dir = str(Path(__file__).resolve().parent.parent)
if parent_dir not in sys.path:
    sys.path.append(parent_dir)

from web3_data_center.clients.opensearch_client import OpenSearchClient

def format_unified_changes(address_changes: Dict[str, Dict[str, Any]]) -> str:
    """Format unified balance changes for display"""
    if not address_changes:
        return "No balance changes found"
    
    output = []
    total_net_change = 0
    
    # Sort addresses by absolute total change to show most significant changes first
    sorted_addresses = sorted(
        address_changes.items(),
        key=lambda x: abs(x[1]['total_change']),
        reverse=True
    )
    
    for address, changes in sorted_addresses:
        total_change = changes['total_change']
        total_net_change += total_change
        change_eth = total_change / 1e18
        
        output.append(f"\nAddress: {address}")
        output.append(f"Total Change: {change_eth:,.18f} ETH")
        output.append(f"Transactions Count: {changes['tx_count']}")
        output.append(f"First Balance: {changes['first_balance'] / 1e18:,.18f} ETH")
        output.append(f"Last Balance: {changes['last_balance'] / 1e18:,.18f} ETH")
    
    output.append(f"\nTotal Net Change Across All Addresses: {total_net_change / 1e18:,.18f} ETH")
    return "\n".join(output)

async def main():
    # Example transaction hashes
    tx_hashes = [
        "0x28ff51f1c84227a1d682e9ed75ee22b80920b4c18c6a860d1da2f57c17cdeac7",
        "0x5d5d2432c8d4e1dd5f4223944e6c7c46c45b0d7a84c8d9d450498f59fb45c9ce",
        "0x84a94b5c8b0f8e0a0d31761c418f8c4491fd59267ce3b32c9e0e36def0c33dad"
    ]
    
    print(f"\nQuerying unified balance changes for {len(tx_hashes)} transactions")
    print("-" * 80)
    
    async with OpenSearchClient() as client:
        try:
            # Get unified balance changes across all transactions
            results = await client.get_unified_balance_changes(tx_hashes)
            print(results)
            
        except Exception as e:
            print(f"Error: {str(e)}")

if __name__ == "__main__":
    asyncio.run(main())
