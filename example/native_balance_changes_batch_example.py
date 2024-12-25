import asyncio
import sys
import os
from pathlib import Path
from typing import Dict, List, Any

# Add the parent directory to sys.path to import the package
parent_dir = str(Path(__file__).resolve().parent.parent)
if parent_dir not in sys.path:
    sys.path.append(parent_dir)

from web3_data_center.clients.opensearch_client import OpenSearchClient

def format_balance_changes(changes: List[Dict[str, Any]]) -> str:
    """Format balance changes for display"""
    if not changes:
        return "No balance changes found"
    
    output = []
    total_change = 0
    
    for change in changes:
        diff = change['difference']
        total_change += diff
        diff_eth = diff / 1e18
        
        output.append(f"  Address: {change['address']}")
        output.append(f"  Change: {diff_eth:,.18f} ETH")
        
    output.append(f"  Total Change: {total_change / 1e18:,.18f} ETH")
    return "\n".join(output)

async def main():
    # Example transaction hashes
    tx_hashes = [
        "0x28ff51f1c84227a1d682e9ed75ee22b80920b4c18c6a860d1da2f57c17cdeac7",
        "0x1fc34cba106a2783ec7dacd31f777ec82db98134b46000e24a155903121a4827",
        "0xdc3ea02e9817a5c1026e27b8fc3b4a305394c2a47909796e3190477629ef548d"
    ]
    
    print(f"\nQuerying balance changes for {len(tx_hashes)} transactions")
    print("-" * 80)
    
    async with OpenSearchClient() as client:
        try:
            # Get balance changes for all transactions in a single query
            results = await client.get_native_balance_changes_batch(tx_hashes)
            print(results)
            
        except Exception as e:
            print(f"Error: {str(e)}")

if __name__ == "__main__":
    asyncio.run(main())
