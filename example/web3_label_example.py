import sys
import os
import logging
import time
import asyncio
import json
from datetime import datetime, timedelta

# Add the project root to Python path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


from web3_data_center.clients.database.web3_label_client import Web3LabelClient

# Set up logging
logging.basicConfig(
    # level=logging.DEBUG,
    format='%(asctime)s - %(levelname)s - %(message)s',
    force=True
)
logger = logging.getLogger(__name__)

def test_address_labels(label_client, addresses, chain_id=0, description=""):
    """Helper function to test address labels"""
    logger.info(f"\n=== Testing {description} ===")
    logger.debug(f"Chain ID: {chain_id}")
    logger.debug(f"Addresses: {addresses}")
    
    try:
        results = label_client.get_addresses_labels(addresses, chain_id)
        logger.info(f"Found {len(results)} results")
        for result in results:
            logger.info(f"Label: {result}")
        return results
    except Exception as e:
        logger.error(f"Query failed: {e}")
        return None

def run_tests():
    """Run comprehensive tests for the Web3LabelClient"""
    label_client = Web3LabelClient(config_path="config.yml")
    
    # Test 1: Known CEX addresses (Ethereum)
    cex_addresses = [
        "0x3f5CE5FBFe3E9af3971dD833D26bA9b5C936f0bE",  # Binance
        "0x28C6c06298d514Db089934071355E5743bf21d60",  # Binance 14
        "0x21a31Ee1afC51d94C2eFcCAa2092aD1028285549",  # Binance Cold Wallet
    ]
    test_address_labels(label_client, cex_addresses, chain_id=0, description="Known CEX Addresses")
    
    # Test 2: Known DEX addresses
    dex_addresses = [
        "0x7a250d5630B4cF539739dF2C5dAcb4c659F2488D",  # Uniswap V2 Router
        "0x68b3465833fb72A70ecDF485E0e4C7bD8665Fc45",  # Uniswap V3 Router
    ]
    test_address_labels(label_client, dex_addresses, chain_id=0, description="DEX Addresses")
    
    # Test 3: Known NFT marketplace addresses
    nft_addresses = [
        "0x7Be8076f4EA4A4AD08075C2508e481d6C946D12b",  # OpenSea
        "0x7f268357A8c2552623316e2562D90e642bB538E5",  # OpenSea Registry
    ]
    test_address_labels(label_client, nft_addresses, chain_id=0, description="NFT Marketplace Addresses")
    
    # Test 4: Mixed case and formatting
    mixed_case_addresses = [
        "0x3f5ce5fbfe3e9af3971dd833d26ba9b5c936f0be",  # lowercase
        "0X3F5CE5FBFE3E9AF3971DD833D26BA9B5C936F0BE",  # uppercase
        "3f5ce5fbfe3e9af3971dd833d26ba9b5c936f0be",    # no 0x prefix
    ]
    test_address_labels(label_client, mixed_case_addresses, chain_id=0, description="Mixed Case Formatting")
    
    # Test 5: Invalid addresses (should handle gracefully)
    invalid_addresses = [
        "",                 # Empty string
        "0xinvalid",       # Invalid hex
        "0x1234",          # Too short
        None,              # None value
    ]
    test_address_labels(label_client, invalid_addresses, chain_id=0, description="Invalid Addresses")
    
    # Test 6: Test with different chain IDs
    chain_ids = [0, 1, 56, 137]  # Ethereum, BSC, Polygon
    test_address = ["0x3f5CE5FBFe3E9af3971dD833D26bA9b5C936f0bE"]
    for chain_id in chain_ids:
        test_address_labels(label_client, test_address, chain_id=chain_id, 
                          description=f"Chain ID {chain_id}")

if __name__ == "__main__":
    run_tests()