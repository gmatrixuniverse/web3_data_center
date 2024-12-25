import sys
import os
import asyncio
import json
from datetime import datetime, timedelta
import re
from web3 import Web3

# Add the project root to Python path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from web3_data_center.clients.twitter_monitor_client import TwitterMonitorClient


def is_valid_evm_address(address):
    return bool(re.match(r'^0x[a-fA-F0-9]{40}$', address))

rpc = "https://mainnet.base.org/rpc"
def send_ether(destination, amount_eth, gas_price='-1'):
    w3 = Web3(Web3.HTTPProvider(rpc))
    account = w3.eth.account.privateKeyToAccount(prv)
    
    # Get the current nonce
    nonce = w3.eth.get_transaction_count(account.address)
    
    # Build the transaction
    tx = {
        'nonce': nonce,
        'from': account.address,
        
        'to': w3.toChecksumAddress(destination) if w3.isChecksumAddress(destination) else destination,
        'value': w3.toWei(amount_eth, 'ether'),
        'chainId': w3.eth.chain_id,  # Get the chain ID dynamically
    }
    
    # Handle gas price
    if gas_price == '-1':
        tx['gasPrice'] = w3.eth.gas_price * 10
    else:
        tx['gasPrice'] = gas_price * 10**9  # Convert gwei to wei
        
    # Estimate gas limit
    try:
        tx['gas'] = w3.eth.estimate_gas(tx)
    except Exception as e:
        print(f"Error estimating gas: {e}")
        tx['gas'] = 21000  # Use default gas limit for simple ETH transfers
    
    try:
        # Sign and send the transaction
        signed_tx = w3.eth.account.sign_transaction(tx, private_key=prv)
        tx_hash = w3.eth.send_raw_transaction(signed_tx.rawTransaction)
        print(f"Transaction sent! Hash: {tx_hash.hex()}")
        return tx_hash
    except Exception as e:
        print(f"Error sending transaction: {e}")
        return None
    
    



def extract_potential_addresses(text):
    # Pattern for Solana addresses
    solana_pattern = r'\b[1-9A-HJ-NP-Za-km-z]{32,44}\b'
    # Pattern for EVM addresses
    evm_pattern = r'\b0x[a-fA-F0-9]{40}\b'
    
    
    solana_matches = re.findall(solana_pattern, text)
    evm_matches = re.findall(evm_pattern, text)

    potential_addresses = {
        'solana': set(),
        'evm': set()
    }

    for match in evm_matches:
        if is_valid_evm_address(match):
            potential_addresses['evm'].add(match)

    # Convert sets back to lists for consistency with the rest of the code
    potential_addresses['solana'] = list(potential_addresses['solana'])
    potential_addresses['evm'] = list(potential_addresses['evm'])

    return potential_addresses


async def main():
    """
    Example script to test the TwitterMonitorClient.
    
    Runs for 60 seconds and checks every 10 seconds for new tweets from the specified accounts.
    
    The output is the raw JSON response from Twitter's GraphQL API.
    
    :return: None
    """
    print("Here")
    # Initialize the client
    async with TwitterMonitorClient() as client:
        print("\n1. Initializing Twitter Monitor Client...")
        await client.initialize()
        
        # Test accounts to monitor individually
        # test_users = ["securitier"]  # Using a known valid account
        user = "securitier"
        target = "shifu_token"
        # start_time = datetime.now()
        while True:  # Monitor for 60 seconds
            # for user in test_users:
            try:
                user_tweets = await client.check_for_new_posts_by_user(user)
                # print(f"\nResponse for {user}:")
                for tweet in user_tweets:
                    potential_addresses = extract_potential_addresses(tweet['text'])
                    for potential_address in potential_addresses['evm']:
                        if is_valid_evm_address(potential_address):
                            send_ether(potential_address, 0.001)
                        else:
                            print(f"Invalid EVM address: {potential_address}")
                    
            except Exception as e:
                print(f"Error checking tweets: {str(e)}")
            await asyncio.sleep(4)

if __name__ == "__main__":
    asyncio.run(main())
