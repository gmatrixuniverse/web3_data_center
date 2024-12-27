#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import sys
import asyncio
import logging
from typing import List, Dict, Any, Optional
from collections import defaultdict
import json
from datetime import datetime, timezone, UTC

# Add the parent directory to the Python path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from web3_data_center.core.data_center import DataCenter

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# DEX addresses
DEX_ADDRESSES = {addr.lower() for addr in [
    # Banana addresses
    '0x3328f7f4a1d1c57c35df56bbf0c9dcafca309c49',
    '0xdB5889E35e379Ef0498aaE126fc2CCE1fbD23216',
    # Ninja address
    '0x80a64c6d7f12c47b7c66c5b4e20e72bc1fcd5d9e',
    # Aimbot address
    '0x50B8f49f4B2E80e09cE8015C4e7A9c277738Fd3d',
    # Unicorn addresses
    '0x68b3465833fb72A70ecDF485E0e4C7bD8665Fc45',
    '0xC36442b4a4522E871399CD717aBDD847Ab11FE88',
    '0x7a250d5630B4cF539739dF2C5dAcb4c659F2488D',
    '0x3fC91A3afd70395Cd496C647d5a6CC9D4B2b7FAD',
    # Wizard address
    '0x6a153cDf5cC58F47c17D6A6b0187C25c86d1acFD'
]}

async def get_token_deployer(dc: DataCenter, token_address: str) -> Optional[str]:
    """Get the deployer address of a token from eth_tokens table."""
    try:
        query = """
        SELECT creator
        FROM eth_tokens
        WHERE address = %s AND chainid = 1
        """
        result = dc.postgres_client.execute_query(query, [token_address])
        return result[0]['creator'] if result else None
    except Exception as e:
        logger.error(f"Error getting token deployer: {str(e)}")
        return None

async def get_token_pairs(dc: DataCenter, token_address: str) -> List[str]:
    """Get trading pairs for a token using calculate_all_pair_addresses."""
    try:
        # Ignore errors when calculating pair addresses
        try:
            pairs = await dc.calculate_all_pair_addresses(token_address)
            return pairs if pairs else []
        except Exception as e:
            logger.debug(f"Could not calculate pair addresses: {str(e)}")
            return []
    except Exception as e:
        logger.error(f"Error getting token pairs: {str(e)}")
        return []

async def analyze_token_profits(dc: DataCenter, token_address: str) -> Optional[Dict[str, Any]]:
    """Analyze profits for a token with additional classification of addresses."""
    try:
        # Convert token address to lowercase
        token_address = token_address.lower()
        logger.info(f"\nAnalyzing token: {token_address}")
        
        # Get token creation transaction
        query = """
        SELECT creator, creationtx
        FROM eth_tokens
        WHERE LOWER(address) = LOWER(%s) AND chainid = 1
        """
        result = dc.postgres_client.execute_query(query, [token_address])
        deployer = result[0]['creator'].lower() if result and result[0]['creator'] else None
        creation_tx = result[0]['creationtx'] if result else None
        
        logger.info(f"Deployer from eth_tokens: {deployer}")
        
        # Get all transfer transactions for this token
        transfer_txs = await dc.get_token_transfer_txs(token_address)
        if not transfer_txs:
            logger.warning(f"No transfer transactions found for token {token_address}")
            return None
        
        # Combine creation tx with transfer txs
        tx_hashes = [creation_tx] if creation_tx else []
        tx_hashes.extend(transfer_txs)
        tx_hashes = list(set(tx_hashes))  # Remove duplicates
        # print(creation_tx in tx_hashes)
        logger.info(f"Total transactions to analyze: {len(tx_hashes)}")
        print("Total transactions to analyze:", len(tx_hashes))
        # print(tx_hashes)
        # Get profit ranking
        try:
            profits = await dc.get_profit_ranking(tx_hashes)
            # print(profits)
            if not profits:
                logger.warning(f"No profit data found for token {token_address}")
                return None
        except Exception as e:
            logger.error(f"Error getting profit ranking: {str(e)}")
            return None
        
        # Get token pairs
        pairs = await get_token_pairs(dc, token_address)
        pair_addresses = {pair.lower() for pair in pairs} if pairs else set()
        logger.info(f"Found {len(pair_addresses)} pair addresses")
        
        # Get traders from token_trading_stats
        query = """
        SELECT DISTINCT LOWER(wallet_address) as wallet_address
        FROM token_trading_stats
        WHERE LOWER(token_address) = LOWER(%s)
        """
        trader_results = dc.postgres_client.execute_query(query, [token_address])
        trader_addresses = {row['wallet_address'] for row in trader_results} if trader_results else set()
        logger.info(f"Found {len(trader_addresses)} trader addresses")
        
        # Enhance profit data with classifications
        enhanced_profits = []
        for profit_data in profits:
            try:
                address = profit_data['address'].lower()
                
                # Check if address is a developer
                is_dev = address == deployer if deployer else False
                if is_dev:
                    logger.info(f"Found developer address: {address}")
                
                # Check if address is developer-related
                is_dev_related = False  # We'll implement this later if needed
                
                # Check if address is a pair
                is_pair = address in pair_addresses
                if is_pair:
                    logger.info(f"Found pair address: {address}")
                
                # Check if address is a trader
                is_trader = address in trader_addresses
                if is_trader:
                    logger.info(f"Found trader address: {address}")
                
                # Add classifications to profit data
                enhanced_profit = {
                    'address': address,
                    'profit': profit_data.get('total_profit_usd', 0),
                    'classifications': {
                        'is_dev': is_dev,
                        'is_dev_related': is_dev_related,
                        'is_pair': is_pair,
                        'is_trader': is_trader,
                        'is_dex': address in DEX_ADDRESSES
                    }
                }
                enhanced_profits.append(enhanced_profit)
            except Exception as e:
                logger.error(f"Error processing profit data for address {profit_data.get('address')}: {str(e)}")
                continue
            
        # Sort profits by amount
        enhanced_profits.sort(key=lambda x: x['profit'], reverse=True)
        
        # Count addresses by classification
        classification_counts = {
            'total_addresses': len(enhanced_profits),
            'developers': sum(1 for p in enhanced_profits if p['classifications']['is_dev']),
            'developer_related': sum(1 for p in enhanced_profits if p['classifications']['is_dev_related']),
            'pair_addresses': sum(1 for p in enhanced_profits if p['classifications']['is_pair']),
            'traders': sum(1 for p in enhanced_profits if p['classifications']['is_trader']),
            'dex_addresses': sum(1 for p in enhanced_profits if p['classifications']['is_dex'])
        }
        
        logger.info("Classification counts:")
        for category, count in classification_counts.items():
            logger.info(f"- {category}: {count}")
        
        return {
            'token_address': token_address,
            'deployer': deployer,
            'classification_counts': classification_counts,
            'top_profits': enhanced_profits,
            'analyzed_at': datetime.now(UTC).isoformat()
        }
            
    except Exception as e:
        logger.error(f"Error analyzing token {token_address}: {str(e)}")
        return None

async def check_token_trader(dc: DataCenter, token_address: str, address: str) -> bool:
    """Check if an address is a trader of the token by querying token_trading_stats table."""
    try:
        query = """
        SELECT COUNT(*) as count
        FROM token_trading_stats
        WHERE token_address = %s AND wallet_address = %s
        """
        result = dc.postgres_client.execute_query(query, [token_address, address])
        return result[0]['count'] > 0 if result else False
    except Exception as e:
        logger.error(f"Error checking token trader: {str(e)}")
        return False

def parse_hex_balance(hex_balance: str) -> int:
    """Parse hex balance, handling empty or invalid values."""
    try:
        if not hex_balance or hex_balance == '0x' or hex_balance == '0x0':
            return 0
        # Remove '0x' prefix and convert to int
        hex_str = hex_balance.replace('0x', '')
        return int(hex_str, 16) if hex_str else 0
    except (ValueError, TypeError) as e:
        logger.debug(f"Could not parse hex balance {hex_balance}: {str(e)}")
        return 0

async def save_to_db(dc: DataCenter, result: Dict[str, Any]):
    """Save analysis results to the database."""
    try:
        # Save classification counts
        query = """
        INSERT INTO token_profit_stats (
            token_address, deployer, total_addresses, developers, developer_related,
            pair_addresses, traders, dex_addresses, analyzed_at
        ) VALUES (
            %s, %s, %s, %s, %s, %s, %s, %s, %s
        ) ON CONFLICT (token_address) DO UPDATE SET
            deployer = EXCLUDED.deployer,
            total_addresses = EXCLUDED.total_addresses,
            developers = EXCLUDED.developers,
            developer_related = EXCLUDED.developer_related,
            pair_addresses = EXCLUDED.pair_addresses,
            traders = EXCLUDED.traders,
            dex_addresses = EXCLUDED.dex_addresses,
            analyzed_at = EXCLUDED.analyzed_at
        """
        
        dc.postgres_client.execute_dml(query, [
            result['token_address'],
            result['deployer'],
            result['classification_counts']['total_addresses'],
            result['classification_counts']['developers'],
            result['classification_counts']['developer_related'],
            result['classification_counts']['pair_addresses'],
            result['classification_counts']['traders'],
            result['classification_counts']['dex_addresses'],
            datetime.now(UTC)
        ])
        
        # Save top profitable addresses
        for rank, profit in enumerate(result['top_profits'], 1):
            query = """
            INSERT INTO token_profit_rankings (
                token_address, address, rank, profit_usd,
                is_dev, is_dev_related, is_pair, is_trader, is_dex
            ) VALUES (
                %s, %s, %s, %s, %s, %s, %s, %s, %s
            ) ON CONFLICT (token_address, address) DO UPDATE SET
                rank = EXCLUDED.rank,
                profit_usd = EXCLUDED.profit_usd,
                is_dev = EXCLUDED.is_dev,
                is_dev_related = EXCLUDED.is_dev_related,
                is_pair = EXCLUDED.is_pair,
                is_trader = EXCLUDED.is_trader,
                is_dex = EXCLUDED.is_dex
            """
            
            dc.postgres_client.execute_dml(query, [
                result['token_address'],
                profit['address'],
                rank,
                profit['profit'],
                profit['classifications']['is_dev'],
                profit['classifications']['is_dev_related'],
                profit['classifications']['is_pair'],
                profit['classifications']['is_trader'],
                profit['classifications']['is_dex']
            ])
            
        logger.info(f"Saved results for token {result['token_address']}")
    except Exception as e:
        logger.error(f"Error saving results to database: {str(e)}")

async def main():
    # Create DataCenter instance
    config_path = os.path.join(
        os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
        'config.yml'
    )
    
    async with DataCenter(config_path=config_path) as dc:
        # Create tables if they don't exist
        create_tables = """
        -- Table for token profit statistics
        CREATE TABLE IF NOT EXISTS token_profit_stats (
            token_address VARCHAR(42) PRIMARY KEY,
            deployer VARCHAR(42),
            total_addresses INTEGER,
            developers INTEGER,
            developer_related INTEGER,
            pair_addresses INTEGER,
            traders INTEGER,
            dex_addresses INTEGER,
            analyzed_at TIMESTAMP WITH TIME ZONE
        );
        
        -- Table for token profit rankings
        CREATE TABLE IF NOT EXISTS token_profit_rankings (
            token_address VARCHAR(42),
            address VARCHAR(42),
            rank INTEGER,
            profit_usd NUMERIC,
            is_dev BOOLEAN,
            is_dev_related BOOLEAN,
            is_pair BOOLEAN,
            is_trader BOOLEAN,
            is_dex BOOLEAN,
            PRIMARY KEY (token_address, address)
        );
        """
        dc.postgres_client.execute_ddl(create_tables)
        # Get tokens from scam_summary table
        query = """
        SELECT token_address 
        FROM scam_summary 
        LIMIT 3
        """
        tokens = dc.postgres_client.execute_query(query)
        if not tokens:
            logger.error("No tokens found in scam_summary table")
            return
            
        # Analyze each token
        logger.info(f"Analyzing {len(tokens)} tokens...")
        for token_row in tokens:
            token_address = token_row['token_address']
            try:
                result = await analyze_token_profits(dc, token_address)
                if result:
                    await save_to_db(dc, result)
            except Exception as e:
                logger.error(f"Error analyzing token {token_address}: {str(e)}")
                continue
        
        logger.info("Analysis complete")

if __name__ == '__main__':
    asyncio.run(main())
