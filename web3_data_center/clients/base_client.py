import os
import yaml
from urllib.parse import urlencode, urlunparse, urlparse, parse_qsl
import aiohttp
from aiohttp import ClientTimeout
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type

import asyncio
import requests
import time
from typing import Dict, Any, Optional
import logging
import json

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class RateLimiter:
    def __init__(self, rpm: int = 120):
        self.rate_limit = rpm
        self.time_period = 60.0  # 1 minute in seconds
        self.tokens = rpm
        self.updated_at = asyncio.get_event_loop().time()
        self.lock = asyncio.Lock()

    async def acquire(self):
        async with self.lock:
            now = asyncio.get_event_loop().time()
            time_passed = now - self.updated_at
            self.tokens += time_passed * (self.rate_limit / self.time_period)
            if self.tokens > self.rate_limit:
                self.tokens = self.rate_limit
            self.updated_at = now

            if self.tokens < 1:
                wait_time = (1 - self.tokens) * (self.time_period / self.rate_limit)
                await asyncio.sleep(wait_time)
                self.tokens = 0
                self.updated_at = asyncio.get_event_loop().time()
            else:
                self.tokens -= 1

class BaseClient:

    def __init__(self, api_name: str, config_path: str = "config.yml", credentials_source: str = "config", api_key_env: Optional[str] = None, use_proxy: bool = False, use_zenrows: bool = False):
        self.config = self.load_config(config_path)
        self.api_name = api_name
        self.base_url = self.config['api'][api_name]['base_url']
        self.credentials = self.load_credentials(credentials_source, api_key_env)
        self.auth_method = self.credentials.get("auth_method", "APIKey")
        self.use_zenrows = use_zenrows
        self.zenrows_api_key = self.config['zenrows']['api_key'] if use_zenrows else None
        self.proxy = 'http://127.0.0.1:7890' if use_proxy else None
        self.headers = self._set_auth_headers()
        rpm = self.config['api'][api_name].get('rpm', 120)  # Default to 120 RPM if not specified
        self.rate_limiter = RateLimiter(rpm)
        self.semaphore = asyncio.Semaphore(self.config['api'][api_name].get('max_concurrent', 5))

    def load_config(self, config_path: str) -> Dict[str, Any]:
        try:
            with open(config_path, 'r') as file:
                return yaml.safe_load(file)
        except FileNotFoundError:
            raise FileNotFoundError("config.yml file not found. Please ensure it exists in the working directory.")
        except yaml.YAMLError as e:
            raise ValueError(f"Error parsing config.yml: {str(e)}")

    def load_credentials(self, source: str, env_var: Optional[str]) -> Dict[str, Any]:
        if source == "config":
            return self.config['api'][self.api_name].get('credentials', {})
        elif source == "env":
            if not env_var:
                raise ValueError("env_var must be provided when loading credentials from environment variables.")
            return {
                'api_key': os.getenv(env_var),
                'auth_method': "APIKey",  # 默认假设是API Key认证
                'api_key_header': os.getenv(f"{env_var}_HEADER", "X-API-KEY")  # 可选地从环境变量中获取头字段名
            }
        else:
            raise ValueError("Invalid credentials source. Choose 'config' or 'env'.")

    def _set_auth_headers(self):
        headers = {
            'accept': 'application/json',
            'user-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36'
        }

        if self.auth_method == "OAuth2":
            headers.update(self._get_oauth2_token())
        elif self.auth_method == "APIKey":
            api_key_header = self.credentials.get("api_key_header", "X-API-KEY")
            headers[api_key_header] = self.credentials['api_key']
        elif self.auth_method == "JWT":
            headers.update(self._get_jwt_token())
        elif self.auth_method == "Basic":
            headers['Authorization'] = aiohttp.BasicAuth(self.credentials['username'], self.credentials['password']).encode()
        elif self.auth_method == "NoAuth":
            pass
        elif self.auth_method == "Param":
            pass
        else:
            raise ValueError(f"Unsupported authentication method: {self.auth_method}")

        return headers

    def _set_auth_params(self, params: Dict[str, Any]):
        if self.auth_method == "Param":
            param_name = self.credentials.get("param_name", "apikey")
            params[param_name] = self.credentials['api_key']
        return params

    def _get_oauth2_token(self) -> Dict[str, str]:
        token_url = self.credentials['token_url']
        client_id = self.credentials['client_id']
        client_secret = self.credentials['client_secret']
        payload = {
            'grant_type': 'client_credentials',
            'client_id': client_id,
            'client_secret': client_secret
        }
        response = requests.post(token_url, data=payload)
        response.raise_for_status()
        token = response.json().get('access_token')
        return {'Authorization': f'Bearer {token}'}

    def _get_jwt_token(self) -> Dict[str, str]:
        import jwt
        secret = self.credentials['secret']
        payload = {
            'exp': time.time() + 3600,  # Token expires in 1 hour
            'username': self.credentials.get('username')
        }
        token = jwt.encode(payload, secret, algorithm='HS256')
        return {'Authorization': f'Bearer {token}'}

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=4, max=10),
        retry=retry_if_exception_type((aiohttp.ClientError, asyncio.TimeoutError, json.JSONDecodeError)),
        reraise=True
    )
    async def _make_request(
        self,
        endpoint: str,
        method: str = "GET",
        params: Optional[Dict[str, Any]] = None,
        data: Optional[Dict[str, Any]] = None,
        headers: Optional[Dict[str, str]] = None,
        timeout: int = 30
    ) -> Dict[str, Any]:
        url = f"{self.base_url}{endpoint}"
        full_headers = {**self.headers, **(headers or {})}
        full_headers['Content-Type'] = 'application/json'
        full_headers['Accept'] = 'application/json'
        params = self._set_auth_params(params)
        if self.use_zenrows:
            url, params = self._prepare_zenrows_request(url, params)

        async with aiohttp.ClientSession(timeout=ClientTimeout(total=timeout)) as session:
            try:
                async with session.request(
                    method=method.upper(),
                    url=url,
                    params=params,
                    json=data,
                    headers=full_headers,
                    proxy=self.proxy
                ) as response:
                    response.raise_for_status()
                    return await self._handle_json_response(response)
            except aiohttp.ClientResponseError as e:
                logging.error(f"HTTP error {e.status}: {e.message}")
                raise
            except aiohttp.ClientError as e:
                logging.error(f"Client error: {str(e)}")
                raise
            except asyncio.TimeoutError:
                logging.error("Request timed out")
                raise
            except json.JSONDecodeError as e:
                logging.error(f"Failed to decode JSON response: {str(e)}")
                raise
            except Exception as e:
                logging.error(f"Unexpected error: {str(e)}")
                raise

    def _prepare_zenrows_request(self, url: str, params: Optional[Dict[str, Any]]) -> tuple:
        from urllib.parse import urlencode
        query_string = urlencode(params or {})
        original_url = f"{url}?{query_string}" if query_string else url
        zenrows_url = "https://api.zenrows.com/v1"
        zenrows_params = {
            "url": original_url,
            "apikey": self.zenrows_api_key,
            "js_render": "true",
            "antibot": "true",
            "premium_proxy": "true"
        }
        return zenrows_url, zenrows_params

    async def _handle_json_response(self, response: aiohttp.ClientResponse) -> Dict[str, Any]:
        try:
            return await response.json(content_type=None)
        except json.JSONDecodeError as e:
            logging.error(f"Failed to decode JSON response: {str(e)}")
            raise

    async def _make_concurrent_requests(
        self,
        requests: list[tuple[str, dict]],  # List of (endpoint, params) tuples
        method: str = "GET",
        timeout: int = 30
    ) -> list[Dict[str, Any]]:
        """
        Make multiple requests concurrently while respecting rate limits.
        
        Args:
            requests: List of (endpoint, params) tuples
            method: HTTP method to use
            timeout: Request timeout in seconds
            
        Returns:
            List of API responses in the same order as the input requests
        """
        async def _rate_limited_request(endpoint: str, params: dict) -> Dict[str, Any]:
            async with self.semaphore:  # Limit concurrent requests
                await self.rate_limiter.acquire()  # Respect rate limits
                return await self._make_request(
                    endpoint=endpoint,
                    method=method,
                    params=params,
                    timeout=timeout
                )
        
        tasks = [
            _rate_limited_request(endpoint, params)
            for endpoint, params in requests
        ]
        
        results = []
        for task in asyncio.as_completed(tasks):
            try:
                result = await task
                results.append(result)
            except Exception as e:
                logger.error(f"Request failed: {str(e)}")
                results.append(e)
                
        return results

# Example config.yml structure
"""
api:
  my_api:
    base_url: "https://api.example.com"
    credentials:
      auth_method: "APIKey"  # 支持 "OAuth2", "JWT", "Basic", "NoAuth"
      api_key: "your-api-key"
      api_key_header: "Authorization"  # 可选，默认是 "X-API-KEY"
      token_url: "https://oauth2.example.com/token"  # 如果是OAuth2认证
      client_id: "your-client-id"
      client_secret: "your-client-secret"
      secret: "your-jwt-secret"  # 如果是JWT认证
      username: "your-username"  # 如果是Basic认证
      password: "your-password"
"""