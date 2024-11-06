import asyncio
import sys
import os

# Add the project root directory to the Python path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from web3_data_center.core.data_center import DataCenter
from web3_data_center.utils.logger import get_logger
import datetime
import pandas as pd
from web3 import Web3
logger = get_logger(__name__)

async def main():
    # Initialize DataCenter
    data_center = DataCenter()

    try:
        # Example 1: Get token info
        # token_address = "CzLSujWBLFsSjncfkh59rUFqvafWcY5tzedWJSuypump"  # Wrapped SOL
        # token_info = await data_center.get_token_info(token_address)
        # logger.info(f"Token Info: {token_info}")

        # Example 2: Get price history
        # symbol, ath, drawdown = await data_center.get_token_call_performance(token_address, chain='sol', called_time=datetime.datetime.now() - datetime.timedelta(hours=3))
        # logger.info(f"Token: {symbol}, ATH: {ath}, Drawdown: {drawdown}")
        # df = pd.read_csv('tokens.csv')
        # tokens = df['token_address'].tolist()
        # results = await data_center.check_tokens_safe(tokens, chain='eth')
        # false_count = results.count(False)
        # false_percentage = (false_count / len(results)) * 100
        # print(f"Percentage of unsafe tokens: {false_percentage:.2f}%")
        # deployed_contracts = await data_center.get_deployed_contracts(address="0x37aAb97476bA8dC785476611006fD5dDA4eed66B", chain='ethereum')
        # logger.info(f"Deployed Contracts: {deployed_contracts}")
        # await data_center.get_latest_swap_txs(address_list=['0x37aAb97476bA8dC785476611006fD5dDA4eed66B'], chain='ethereum')
        # txs = await data_center.get_latest_swap_txs()
        txses = await data_center.get_latest_txs_with_logs(address_list=['0x37aAb97476bA8dC785476611006fD5dDA4eed66B'], chain='ethereum')
        logger.info(txses)
            # contract_tx_count = await data_center.get_contract_tx_count(address=w3.to_checksum_address(contract), chain='ethereum')
            # logger.info(f"Contract TX Count: {contract_tx_count}")
        # # Example 3: Get top holders
        # top_holders = await data_center.get_top_holders(token_address, limit=10)
        # logger.info(f"Top 5 Holders: {top_holders}")

        # # Example 4: Get hot tokens
        # hot_tokens = await data_center.get_hot_tokens(limit=10)
        # logger.info(f"Hot Tokens: {hot_tokens}")

        # # Example 8: Get new pairs
        # new_pairs = await data_center.get_new_pairs(chain='sol', limit=10)
        # logger.info(f"New Pairs: {new_pairs}")

        # # Example 9: Get wallet data
        # wallet_address = "9EPM2qxWYTcpHXDNXGGXXGQ5fWVBRXooDYVwXXmJLchc"  # Example Solana wallet
        # wallet_data = await data_center.get_wallet_data(wallet_address, chain='sol')
        # logger.info(f"Wallet Data: {wallet_data}")


    except Exception as e:
        logger.error(f"An error occurred: {str(e)}")
    finally:
        # Close the DataCenter
        await data_center.close()

if __name__ == "__main__":
    asyncio.run(main())
