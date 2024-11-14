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
        # Read the CSV file
        df = pd.read_csv('tokens_with_50plus_initial_snipers.csv')
        w3 = Web3()

        # Process each token
        # for index, row in df.iterrows():
        #     token_address = w3.to_checksum_address(row['token_address'])
        #     try:
        #         rugged_status = await data_center.is_token_rugged(token_address, chain=1)
        #         logger.info(f"Token {token_address}: Rugged Status = {rugged_status}")
        #     except Exception as e:
        #         logger.error(f"Error processing token {token_address}: {str(e)}")
        #         continue
        status = await data_center.is_token_rugged(token_contract='0x4407340222823c57B2dE6843dc169Adb7fba2a1D', chain=1)
        logger.info(f"Token: Rugged Status = {status}")

    except Exception as e:
        logger.error(f"An error occurred: {str(e)}")
    finally:
        # Close the DataCenter
        await data_center.close()


if __name__ == "__main__":
    asyncio.run(main())
