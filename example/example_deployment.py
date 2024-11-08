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
        token_contract = '0x240cd7b53d364a208ed41f8ced4965d11f571b7a'
        deployed_block = await data_center.get_deployed_block(token_contract)
        logger.info(deployed_block)

        pair_address = '0x7eb6d3466600b4857eb60a19e0d2115e65aa815e'
        orders = await data_center.get_token_pair_orders_between(token_contract, pair_address, deployed_block, deployed_block+200)
        logger.info(orders)

        # pairs = await data_center.get_pairs_info(token_contract)
        # logger.info(pair_address)
    except Exception as e:
        logger.error(f"An error occurred: {str(e)}")
    finally:
        # Close the DataCenter
        await data_center.close()

if __name__ == "__main__":

    asyncio.run(main())