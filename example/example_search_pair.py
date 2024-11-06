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
        pair = await data_center.get_best_pair(contract_address='0x1121AcC14c63f3C872BFcA497d10926A6098AAc5')
        logger.info(pair['pairAddress'])
        info = await data_center.get_token_info(chain='eth', address='0x1121AcC14c63f3C872BFcA497d10926A6098AAc5')
        logger.info(info)

    except Exception as e:
        logger.error(f"An error occurred: {str(e)}")
    finally:
        # Close the DataCenter
        await data_center.close()

if __name__ == "__main__":
    asyncio.run(main())