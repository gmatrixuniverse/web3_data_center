import asyncio
import sys
import os


# Add the project root directory to the Python path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


from web3_data_center.utils.content_extractor import ContentExtractor
from web3_data_center.utils.logger import get_logger
import datetime
import pandas as pd
from web3_data_center.models.source import Source, SourceType
from web3 import Web3
import aiohttp

logger = get_logger(__name__)


async def main():
    async with aiohttp.ClientSession() as http_client:
        extractor = ContentExtractor(http_client, use_proxy=True)
        src = Source(SourceType.TWEET, "1861367286893850665")
        try:
            # Example 1: Get token info
            res = await extractor.extract_from_source(src, depth=3)
            print(res)
        except Exception as e:
            print(f"Error: {e}")

if __name__ == "__main__":
    asyncio.run(main())