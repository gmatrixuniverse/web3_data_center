from typing import Dict, Any, Optional, List
from .base_client import BaseClient
from ..models.token import Token
from ..models.holder import Holder
from ..models.transaction import Transaction
from ..models.price_history_point import PriceHistoryPoint
from ..models.token_security import TokenSecurity
from dataclasses import dataclass
from enum import Enum
import time
import asyncio

class SortBy(Enum):
    VOLUME = "volume"
    TRADE = "trade"

class SortType(Enum):
    ASC = "asc"
    DESC = "desc"

class TimeFrame(Enum):
    THIRTY_MIN = "30m"
    ONE_HOUR = "1h"
    TWO_HOURS = "2h"
    FOUR_HOURS = "4h"
    SIX_HOURS = "6h"
    EIGHT_HOURS = "8h"
    TWELVE_HOURS = "12h"
    TWENTY_FOUR_HOURS = "24h"

class AddressType(Enum):
    TOKEN = "token"
    POOL = "pool"

class IntervalType(Enum):
    FIFTEEN_MIN = "15m"
    ONE_HOUR = "1h"
    FOUR_HOURS = "4h"
    ONE_DAY = "1d"

class BirdeyeClient(BaseClient):
    def __init__(self, config_path: str = "config.yml", use_proxy: bool = False):
        super().__init__('birdeye', config_path=config_path, use_proxy=use_proxy)

    async def get_token_info(self, token_address: str) -> Optional[Token]:
        """
        Retrieve an overview of a specific token based on its address.

        Args:
            token_address (str): The address of the token for which you want to retrieve an overview.

        Returns:
            Optional[Token]: A Token object containing the token overview,
                             or None if the request fails or data is not available.
        """
        endpoint = "/defi/token_overview"
        params = {"address": token_address}
        response = await self._make_request(endpoint, params=params)
        
        if response and isinstance(response, dict) and 'data' in response:
            data = response['data']
            return Token(
                address=data.get('address'),
                name=data.get('name'),
                symbol=data.get('symbol'),
                decimals=data.get('decimals'),
                total_supply=data.get('supply'),
                price=data.get('price'),
                holder_count=data.get('uniqueWallet24h'),
                market_cap=data.get('mc'),
                liquidity=data.get('liquidity'),
                volume_24h=data.get('v24h'),
                swap_count_24h=data.get('trade24h'),
                created_at=None,  # Birdeye API doesn't provide creation time
                chain='solana'
            )
        else:
            return None

    async def get_token_security(self, token_address: str) -> Optional[TokenSecurity]:
        """
        Get comprehensive security information about a specified token.

        Args:
            token_address (str): The address of the token for which security information is requested.

        Returns:
            Optional[TokenSecurity]: A TokenSecurity object containing the token security information,
                                     or None if the request fails or data is not available.
        """
        endpoint = f"/defi/token_security"
        headers = {'x-chain': 'solana'}
        params = {"address": token_address}
        response = await self._make_request(endpoint, params=params, headers=headers)
        
        if response and isinstance(response, dict) and 'data' in response:
            data = response['data']
            return TokenSecurity(
                creator_address=data.get('creatorAddress'),
                owner_address=data.get('ownerAddress'),
                creation_tx=data.get('creationTx'),
                creation_time=data.get('creationTime'),
                creation_slot=data.get('creationSlot'),
                mint_tx=data.get('mintTx'),
                mint_time=data.get('mintTime'),
                mint_slot=data.get('mintSlot'),
                creator_balance=data.get('creatorBalance'),
                owner_balance=data.get('ownerBalance'),
                owner_percentage=data.get('ownerPercentage'),
                creator_percentage=data.get('creatorPercentage'),
                metaplex_update_authority=data.get('metaplexUpdateAuthority'),
                metaplex_update_authority_balance=data.get('metaplexUpdateAuthorityBalance'),
                metaplex_update_authority_percent=data.get('metaplexUpdateAuthorityPercent'),
                mutable_metadata=data.get('mutableMetadata'),
                top10_holder_balance=data.get('top10HolderBalance'),
                top10_holder_percent=data.get('top10HolderPercent'),
                top10_user_balance=data.get('top10UserBalance'),
                top10_user_percent=data.get('top10UserPercent'),
                is_true_token=data.get('isTrueToken'),
                total_supply=data.get('totalSupply'),
                pre_market_holder=data.get('preMarketHolder'),
                lock_info=data.get('lockInfo'),
                freezeable=data.get('freezeable'),
                freeze_authority=data.get('freezeAuthority'),
                transfer_fee_enable=data.get('transferFeeEnable'),
                transfer_fee_data=data.get('transferFeeData'),
                is_token_2022=data.get('isToken2022'),
                non_transferable=data.get('nonTransferable')
            )
        else:
            return None

    async def get_price_history(
        self,
        address: str,
        address_type: str = "token",
        interval: str = "15m",
        time_from: Optional[int] = None,
        time_to: Optional[int] = None,
        max_records: int = 1000
    ) -> List[PriceHistoryPoint]:
        """
        Fetch historical token prices for a specific token address.

        Args:
            address (str): The token address to retrieve historical prices for.
            address_type (str): The type of address provided (default: "token").
            interval (str): The OHLCV interval type (default: "15m"). Must be one of "1m", "5m", or "15m".
            time_from (Optional[int]): The Unix timestamp for the start of the desired time range.
            time_to (Optional[int]): The Unix timestamp for the end of the desired time range.
            max_records (int): The maximum number of records to return (default 1000, max 1000).

        Returns:
            List[PriceHistoryPoint]: A list of PriceHistoryPoint objects containing historical price data.
        """
        endpoint = "/defi/history_price"
        # Set default time range if not provided
        if time_to is None:
            time_to = int(time.time())
        if time_from is None:
            time_from = time_to - (86400 * 7)  # Default to 7 days

        # Ensure interval is valid
        if interval not in ["1m", "5m", "15m"]:
            raise ValueError("Interval must be one of '1m', '5m', or '15m'")

        params = {
            "address": address,
            "address_type": address_type,
            "type": interval,
            "time_from": time_from,
            "time_to": time_to
        }
        response = await self._make_request(endpoint, params=params)
        if response and isinstance(response, dict) and 'data' in response and 'items' in response['data']:
            items = response['data']['items']
            return [
                PriceHistoryPoint.from_dict(item)
                for item in items[:max_records]
            ]
        else:
            return []

    async def get_top_traders(self, 
                        token_address: str, 
                        sort_by: SortBy = SortBy.VOLUME, 
                        sort_type: SortType = SortType.DESC, 
                        time_frame: TimeFrame = TimeFrame.TWENTY_FOUR_HOURS, 
                        offset: int = 0, 
                        limit: int = 10) -> Optional[List[Holder]]:
        """
        Get top traders for a specific token.

        Args:
            token_address (str): The address of the token.
            sort_by (SortBy): The attribute to sort the traders by. Default is VOLUME.
            sort_type (SortType): The order to sort the traders in. Default is DESC.
            time_frame (TimeFrame): The time frame for the top traders data. Default is TWENTY_FOUR_HOURS.
            offset (int): The number of records to skip from the start. Default is 0.
            limit (int): The maximum number of records to return. Must be between 1 and 10. Default is 10.

        Returns:
            Optional[List[Holder]]: A list of Holder objects, or None if the request fails.
        """
        endpoint = "/defi/v2/tokens/top_traders"
        params = {
            "address": token_address,
            "sort_by": sort_by.value,
            "sort_type": sort_type.value,
            "time_frame": time_frame.value,
            "offset": offset,
            "limit": min(max(limit, 1), 10)  # Ensure limit is between 1 and 10
        }
        headers = {'x-chain': 'solana'}  # Update this as needed for other chains
        response = await self._make_request(endpoint, params=params, headers=headers)
        
        if response and isinstance(response, dict) and 'data' in response and 'items' in response['data']:
            return [
                Holder(
                    address=item['owner'],
                    token_address=item['tokenAddress'],
                    amount=item['volume'],
                    total_trades=item['tradeBuy']+item['tradeSell'],
                    buy_count=item['tradeBuy'],
                    sell_count=item['tradeSell'],
                    buy_volume=item['volumeBuy'],
                    sell_volume=item['volumeSell'],
                    tags=item.get('tags', [])
                )
                for item in response['data']['items']
            ]
        else:
            return None

    async def get_all_top_traders(self, 
                            token_address: str, 
                            sort_by: SortBy = SortBy.VOLUME, 
                            sort_type: SortType = SortType.DESC, 
                            time_frame: TimeFrame = TimeFrame.TWENTY_FOUR_HOURS, 
                            max_traders: int = 100) -> List[Holder]:
        """
        Get all top traders for a specific token, up to a maximum number.

        Args:
            token_address (str): The address of the token.
            sort_by (SortBy): The attribute to sort the traders by. Default is VOLUME.
            sort_type (SortType): The order to sort the traders in. Default is DESC.
            time_frame (TimeFrame): The time frame for the top traders data. Default is TWENTY_FOUR_HOURS.
            max_traders (int): The maximum number of traders to retrieve. Default is 100.

        Returns:
            List[Holder]: A list of Holder objects.
        """
        all_traders = []
        offset = 0
        limit = 10  # API allows max 10 items per request

        while len(all_traders) < max_traders:
            traders = await self.get_top_traders(token_address, sort_by, sort_type, time_frame, offset, limit)
            if traders:
                all_traders.extend(traders)
                offset += limit
            else:
                break

        return all_traders[:max_traders]
    
    async def get_token_price_at_time(self, token_address: str, time: int) -> Optional[TokenSecurity]:
        """
        Get the price of a token at a specific time.

        Args:
            token_address (str): The address of the token for which the price is requested.
            time (int): The timestamp to get the price at.
        Returns:
            Optional[float]: The price at the given time, or None if the request fails or data is not available.
        """
        endpoint = f"/defi/history_price"
        headers = {'x-chain': 'solana'}
        params = {"address": token_address, "time": time}
        response = await self._make_request(endpoint, params=params, headers=headers)
        
        if response and isinstance(response, dict) and 'data' in response:
            data = response['data']
            return data.get('close')
        else:
            return None
