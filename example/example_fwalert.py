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

async def trigger_alert():
    # Get the singleton instance
    fwclient = FWAlertClient.get_instance(config_path="config.yml", use_proxy=False)
    await fwclient.callme("Vitalik发新推文了")


async def main():
    # Run multiple alerts
    await trigger_alert()

if __name__ == '__main__':
    asyncio.run(main())
