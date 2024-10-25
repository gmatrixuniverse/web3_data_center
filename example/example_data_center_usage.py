import asyncio
import sys
import os

# Add the project root directory to the Python path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from web3_data_center.core.data_center import DataCenter
from web3_data_center.utils.logger import get_logger
import datetime

logger = get_logger(__name__)

async def main():
    # Initialize DataCenter
    data_center = DataCenter()

    try:
        # Example 1: Get token info
        token_address = "CzLSujWBLFsSjncfkh59rUFqvafWcY5tzedWJSuypump"  # Wrapped SOL
        token_info = await data_center.get_token_info(token_address)
        # logger.info(f"Token Info: {token_info}")

        # Example 2: Get price history
        symbol, ath, drawdown = await data_center.get_token_call_performance(token_address, chain='sol', called_time=datetime.datetime.now() - datetime.timedelta(hours=3))
        # logger.info(f"Token: {symbol}, ATH: {ath}, Drawdown: {drawdown}")

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
