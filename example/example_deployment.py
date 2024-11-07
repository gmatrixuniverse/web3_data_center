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
        token_contract = '0x2025bf4e0c1117685b1bf2ea2be56c7deb11bc99'
        # deployed_block = await data_center.get_deployed_block(token_contract)
        # pair_address = '0xBF36AbDf1ac7536adC354FB5A0DeDb4C155520d3'
        # orders = await data_center.get_token_pair_orders_between(token_contract, pair_address, deployed_block, 99999999)
        # logger.info(deployed_block)
        # logger.info(orders)

        pairs = await data_center.get_pairs_info(token_contract)
        logger.info(pairs)
    except Exception as e:
        logger.error(f"An error occurred: {str(e)}")
    finally:
        # Close the DataCenter
        await data_center.close()

if __name__ == "__main__":

    asyncio.run(main())