from typing import List, Dict, Any, Optional
from .base_client import BaseClient
import logging

logger = logging.getLogger(__name__)

class EtherscanClient(BaseClient):
    def __init__(self, config_path: str = "config.yml", use_proxy: bool = True):
        super().__init__('etherscan', config_path=config_path, use_proxy=use_proxy)

    async def get_deployments(self, address_list: List[str], chain: str = 'eth') -> Optional[Dict[str, Any]]:
        addresses = ','.join(address_list)
        endpoint = f"/api"
        params = {
            "module": "contract",
            "action": "getcontractcreation",
            "contractaddresses": addresses
        }
        try:
            response = await self._make_request(endpoint=endpoint, params=params)
            return response['result']
        except Exception as e:
            logger.error(f"Error fetching deployed time for {addresses}: {e}")
            return None

    async def get_deployment(self, address: str, chain: str = 'eth') -> Optional[Dict[str, Any]]:
        endpoint = f"/api"
        params = {
            "module": "contract",
            "action": "getcontractcreation",
            "contractaddresses": address
        }
        try:
            response = await self._make_request(endpoint=endpoint, params=params)
            return response['result'][0]
        except Exception as e:
            logger.error(f"Error fetching deployed time for {address}: {e}")
            return None



