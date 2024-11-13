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
        w3 = Web3()
        # pair_address = w3.to_checksum_address('0x522CB9ACE84961057dC63D29d64ad371201bCAe1')
        # rugged_pairs = await data_center.is_pair_rugged(pair_address=pair_address, pair_type='uni_v2', chain='eth')
        # logger.info(f"Rugged Pairs: {rugged_pairs}")
        token_contract = '0xf35Dd7F8fEB9f5CFc43F8F6F340e787dB33AbEdf'
        rugged_tokens = await data_center.is_token_rugged(token_contract, 1)
        logger.info(f"Rugged Tokens: {rugged_tokens}")

    except Exception as e:
        logger.error(f"An error occurred: {str(e)}")
    finally:
        # Close the DataCenter
        await data_center.close()

if __name__ == "__main__":
    asyncio.run(main())
