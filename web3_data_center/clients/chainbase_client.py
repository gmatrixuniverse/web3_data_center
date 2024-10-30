from typing import List, Dict, Any
from .base_client import BaseClient

class ChainbaseClient(BaseClient):
    def __init__(self, config_path: str = "config.yml", use_proxy: bool = True):
        super().__init__('chainbase', config_path=config_path, use_proxy=use_proxy)

    async def query(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        if not payload:
            return {}

        endpoint = "/dw/query"
        return await self._make_request(endpoint, method="POST", data=payload)
