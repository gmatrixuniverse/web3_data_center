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

print("Hello world 4")

logger = get_logger(__name__)

async def main():
    # Initialize DataCenter
    data_center = DataCenter()
    try:
        token_contract = '0x2025bf4e0c1117685b1bf2ea2be56c7deb11bc99'
        pair = await data_center.get_best_pair(contract_address=token_contract)
        logger.info(pair['pairAddress'])
        orders = await data_center.get_token_pair_orders_at_block(token_contract=token_contract, pair_address=pair['pairAddress'])
        logger.info(orders)

    except Exception as e:
        logger.error(f"An error occurred: {str(e)}")
    finally:
        # Close the DataCenter
        await data_center.close()

if __name__ == "__main__":

    asyncio.run(main())