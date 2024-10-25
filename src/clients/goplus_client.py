from typing import Dict, Any
from .base_client import BaseClient

class GoPlusClient(BaseClient):
    def __init__(self, config_path: str = "config.yml", use_proxy: bool = False):
        super().__init__('goplus', config_path=config_path, use_proxy=use_proxy)

    async def get_token_security(self, chain_id: str, token_address: str) -> Dict[str, Any]:
        endpoint = f"/v1/token_security/{chain_id}"
        params = {"contract_addresses": token_address}
        return await self._make_request(endpoint, params=params)

    def is_token_safe(self, security_info: Dict[str, Any]) -> bool:
        # This is a basic implementation. You may want to adjust the criteria based on your needs.
        if not security_info or 'result' not in security_info:
            return False

        # Check if security_info['result'] is empty
        if not security_info['result']:
            return False

        token_data = next(iter(security_info['result'].values()))
        return (
            token_data.get('is_honeypot', '1') == '0' and
            token_data.get('is_proxy', '0') == '0'
            # token_data.get('can_take_back_ownership', '0') == '0' 
        )