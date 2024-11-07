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
        hash = "0xd27ead1c74ffcf8a50aefb0debf1407be0e1a7b7527a5dc6056efaf2756034ad"
        tx = data_center.w3_client.eth.get_transaction(hash)
        logger.info(tx)
        logs = data_center.w3_client.eth.get_transaction_receipt(hash)
        logger.info(logs)

    except Exception as e:
        logger.error(f"An error occurred: {str(e)}")
    finally:
        # Close the DataCenter
        await data_center.close()

if __name__ == "__main__":

    asyncio.run(main())