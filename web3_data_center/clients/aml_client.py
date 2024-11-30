from typing import Dict, Any, List, Optional
from .base_client import BaseClient
import logging

logger = logging.getLogger(__name__)

class AMLClient(BaseClient):
    """BlockSec AML API Client"""
    
    def __init__(self, config_path: str = "config.yml", use_proxy: bool = False):
        super().__init__('blocksec_aml', config_path=config_path, use_proxy=use_proxy)
        
    async def get_supported_chains(self) -> Dict[str, Any]:
        """Get the list of supported chains for the Address Labels APIs."""
        endpoint = "/chain-list"
        return await self._make_request(endpoint, method="GET")

    async def get_address_labels(self, chain_id: int, address: str) -> Dict[str, Any]:
        """Retrieve detailed label information of a specific address on a particular chain."""
        endpoint = "/labels"
        data = {
            "chain_id": chain_id,
            "address": address
        }
        return await self._make_request(endpoint, method="POST", data=data)

    async def get_batch_address_labels(self, chain_id: int, addresses: List[str]) -> Dict[str, Any]:
        """Retrieve detailed label information of a list of addresses on a particular chain."""
        if len(addresses) > 100:
            raise ValueError("Maximum of 100 addresses allowed per request")
            
        endpoint = "/batch-labels"
        data = {
            "chain_id": chain_id,
            "addresses": addresses
        }
        return await self._make_request(endpoint, method="POST", data=data)

    async def get_entity_info(self, entity: str) -> Dict[str, Any]:
        """Get information about a specific entity."""
        endpoint = "/entity"
        data = {
            "entity": entity
        }
        return await self._make_request(endpoint, method="POST", data=data)
