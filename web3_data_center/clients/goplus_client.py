from typing import Dict, Any, List
from .base_client import BaseClient
import logging

logger = logging.getLogger(__name__)
class GoPlusClient(BaseClient):
    def __init__(self, config_path: str = "config.yml", use_proxy: bool = False):
        super().__init__('goplus', config_path=config_path, use_proxy=use_proxy)

    async def get_tokens_security(self, chain_id: str, token_address_list: List[str]) -> Dict[str, Any]:
        endpoint = f"/v1/token_security/{chain_id}"
        requests = [(endpoint, {"contract_addresses": token_address}) for token_address in token_address_list]
        return await self._make_concurrent_requests(
            requests,
            method="GET",
            timeout=10
        )

    async def check_tokens_safe(self, chain_id: str, token_address_list: List[str]) -> List[bool]:
        security_info = await self.get_tokens_security(chain_id=chain_id, token_address_list=token_address_list)
        return [self.is_token_safe(security) for security in security_info]

    def is_token_safe(self, security_info: Dict[str, Any]) -> bool:
        # This is a basic implementation. You may want to adjust the criteria based on your needs.
        if not security_info or 'result' not in security_info:
            logger.error(f"Security info is not valid: {security_info}")
            return False

        # Check if security_info['result'] is empty
        if not security_info['result']:
            logger.error(f"Security info is empty: {security_info}")
            return False

        token_data = next(iter(security_info['result'].values()))
        # print(token_data)
        return (
            token_data.get('is_honeypot', '1') == '0' and
            token_data.get('is_proxy', '1') == '0'
            # token_data.get('can_take_back_ownership', '0') == '0' 
        )
