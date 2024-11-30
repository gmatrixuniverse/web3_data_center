from typing import Dict, Any, List, Optional
from .base_client import BaseClient
import logging

logger = logging.getLogger(__name__)

class FundingClient(BaseClient):
    """Client for interacting with the Funding JSON-RPC API"""
    
    def __init__(self, config_path: str = "config.yml", use_proxy: bool = False):
        super().__init__('funding', config_path=config_path, use_proxy=use_proxy)
        
    async def simulate_view_first_fund(self, address: str) -> Dict[str, Any]:
        """Simulate and view the first fund for a given address."""
        endpoint = "/"  # JSON-RPC endpoint is at root
        data = {
            "method": "simulate_viewFirstFund",
            "params": [address],
            "id": 1,
            "jsonrpc": "2.0"
        }
        return await self._make_request(endpoint, method="POST", data=data)

    async def batch_simulate_view_first_fund(self, addresses: List[str]) -> List[Dict[str, Any]]:
        """Batch simulate and view first fund for multiple addresses."""
        endpoint = "/"
        # Create a batch of JSON-RPC requests
        batch_data = [
            {
                "method": "simulate_viewFirstFund",
                "params": [address],
                "id": i + 1,
                "jsonrpc": "2.0"
            }
            for i, address in enumerate(addresses)
        ]
        return await self._make_request(endpoint, method="POST", data=batch_data)