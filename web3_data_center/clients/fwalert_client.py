from typing import List, Dict, Any
from .base_client import BaseClient

class FWAlertClient(BaseClient):
    def __init__(self, config_path: str = "config.yml", use_proxy: bool = False):
        super().__init__('fwalert', config_path=config_path, use_proxy=use_proxy)

    async def callme(self, topic) -> Dict[str, Any]:
        # include topic in params and set as actual params
        actual_params = {"topic": topic}
        endpoint = self.config['api']['fwalert']['default_endpoint']
        return await self._make_request(endpoint, method="GET", params=actual_params)

    async def notify(self, slug, params) -> Dict[str, Any]:
        endpoint = "/"+ slug
        return await self._make_request(endpoint, method="GET", params=params)
