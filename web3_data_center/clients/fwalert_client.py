from typing import List, Dict, Any
from .base_client import BaseClient

class FWAlertClient(BaseClient):
    _instances = {}

    def __new__(cls, config_path: str = "config.yml", use_proxy: bool = False):
        # Create a unique key for this configuration
        instance_key = (config_path, use_proxy)
        
        if instance_key not in cls._instances:
            cls._instances[instance_key] = super().__new__(cls)
            # Mark as not initialized
            cls._instances[instance_key]._initialized = False
        
        return cls._instances[instance_key]

    def __init__(self, config_path: str = "config.yml", use_proxy: bool = False):
        if not getattr(self, '_initialized', False):
            super().__init__('fwalert', config_path=config_path, use_proxy=use_proxy)
            self._initialized = True

    async def callme(self, topic) -> Dict[str, Any]:
        # include topic in params and set as actual params
        actual_params = {"topic": topic}
        endpoint = self.config['api']['fwalert']['default_endpoint']
        return await self._make_request(endpoint, method="GET", params=actual_params)

    async def notify(self, slug, params) -> Dict[str, Any]:
        endpoint = "/"+ slug
        return await self._make_request(endpoint, method="GET", params=params)

    @classmethod
    def get_instance(cls, config_path: str = "config.yml", use_proxy: bool = False) -> 'FWAlertClient':
        """Get or create a singleton instance of FWAlertClient with specific configuration"""
        return cls(config_path=config_path, use_proxy=use_proxy)
