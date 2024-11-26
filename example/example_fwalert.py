import asyncio
import sys
import os


# Add the project root directory to the Python path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


from web3_data_center.core.data_center import DataCenter
from web3_data_center.clients.fwalert_client import FWAlertClient
from web3_data_center.utils.logger import get_logger
import datetime
import pandas as pd
from web3 import Web3


async def main():
    fwclient = FWAlertClient(config_path="config.yml", use_proxy=False)
    # await fwclient.callme(f"Vitalik发新推文了")
    await fwclient.notify("ddd930ea-2719-4c90-94ac-14bfdb59200d", {"topic": "有新的Pump出现", "contract": "0xsadaddsa"})

if __name__ == '__main__':
    asyncio.run(main())
