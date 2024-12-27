#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import sys
import asyncio
import logging
from typing import List
from collections import defaultdict

# Add the parent directory to the Python path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from web3_data_center.core.data_center import DataCenter
from balance_changes_example import format_profit_ranking

# Configure logging
logging.basicConfig(level=logging.INFO)

async def main():
    # Create DataCenter instance
    config_path = os.path.join(
        os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
        'config.yml'
    )
    
    async with DataCenter(config_path=config_path) as dc:
        # Token address to analyze
        token_address = "0x584fF33A62DB2dC3D123070fC8D24Adbc59b6190".lower()
        
        print(f"\nGetting transactions for token: {token_address}")
        print('-' * 80)
        
        # Get all transactions for this token
        tx_hashes = await dc.get_token_transfer_txs(token_address)
        
        if tx_hashes:
            print(f"\nFound {len(tx_hashes)} transactions")
            print("\nAnalyzing profits for these transactions...")
            profits = await dc.get_profit_ranking(tx_hashes)
            
            # Print profits with their transactions
            print("\nTop Profits:")
            print("-" * 80 + "\n")
            
            # Sort profits by total_profit_usd in descending order
            sorted_profits = sorted(profits, key=lambda x: x.get('total_profit_usd', 0), reverse=True)
            
            # Print top 5 profitable addresses
            profitable_addresses = [p for p in sorted_profits if p.get('total_profit_usd', 0) > 0][:5]  # Limit to top 5
            print(f"\nTop {len(profitable_addresses)} Profitable Addresses:")
            print("-" * 80 + "\n")
            
            for rank, profit_data in enumerate(profitable_addresses, 1):
                address = profit_data.get('address')
                total_profit = profit_data.get('total_profit_usd', 0)
                profit_breakdown = profit_data.get('profit_breakdown', {})
                related_txs = profit_data.get('related_transactions', [])
                
                print(f"Rank #{rank}: {address}")
                print(f"Total Profit: ${total_profit:,.2f}")
                print("Profit Breakdown:")
                for token, details in profit_breakdown.items():
                    symbol = details.get('symbol', 'Unknown')
                    amount = details.get('amount', 0)
                    amount_usd = details.get('amount_usd', 0)
                    print(f"  {symbol}: {amount:,.8f} (${amount_usd:,.2f})")
                
                print("\nRelated Transactions:")
                for tx in related_txs[:5]:  # Show first 5 transactions
                    print(f"\n- Transaction: {tx['hash']}")
                    if tx['eth_change'] != 0:
                        print(f"  ETH Change: {tx['eth_change']:,.8f}")
                    for token_addr, token_data in tx['token_changes'].items():
                        print(f"  {token_data['symbol']} Change: {token_data['amount']:,.8f}")
                if len(related_txs) > 5:
                    print(f"\n... and {len(related_txs) - 5} more transactions")
                print("\n")

            print("Losses:")
            print("-" * 80 + "\n")
            
            # Print addresses with losses
            loss_addresses = [p for p in sorted_profits if p.get('total_profit_usd', 0) < 0]
            print(f"Found {len(loss_addresses)} addresses with losses\n")  # Just show the count
        else:
            print("No transactions found for this token")

if __name__ == '__main__':
    asyncio.run(main())
