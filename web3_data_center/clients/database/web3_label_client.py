from typing import List, Dict, Any, Optional
import logging
import re
import time
from .base_database_client import BaseDatabaseClient

logger = logging.getLogger(__name__)
logger.setLevel(logging.WARNING)  # Set default level to INFO

class Web3LabelClient:
    def __init__(self, db_client: BaseDatabaseClient = None, config_path: str = None, cache_ttl: int = 3600):
        """
        Initialize Web3LabelClient with a database client.
        
        Args:
            db_client: Pre-configured database client
            config_path: Optional path to config file
            cache_ttl: Cache time-to-live in seconds (default: 1 hour)
        """
        if not db_client and not config_path:
            raise ValueError("Either db_client or config_path must be provided")
            
        from .postgresql_client import PostgreSQLClient
        self.db_client = db_client or PostgreSQLClient(config_path=config_path, db_section='labels')
        self._label_cache = {}  # Cache for label results
        self._cache_timestamps = {}  # Timestamps for cache entries
        self._cache_ttl = cache_ttl
        self._address_pattern = re.compile(r'^0x[a-fA-F0-9]{40}$')
            
    def _validate_address(self, address: str) -> bool:
        """Validate Ethereum address format"""
        if not isinstance(address, str):
            return False
        return bool(self._address_pattern.match(address))
    
    def _is_cache_valid(self, address: str) -> bool:
        """Check if cache entry is still valid"""
        if address not in self._cache_timestamps:
            return False
        return (time.time() - self._cache_timestamps[address]) < self._cache_ttl
    
    def _update_cache(self, address: str, info: Dict[str, Any]):
        """Update cache with new information"""
        self._label_cache[address] = info
        self._cache_timestamps[address] = time.time()
    
    def _clean_expired_cache(self):
        """Remove expired cache entries"""
        current_time = time.time()
        expired_addresses = [
            addr for addr, timestamp in self._cache_timestamps.items()
            if (current_time - timestamp) >= self._cache_ttl
        ]
        for addr in expired_addresses:
            del self._label_cache[addr]
            del self._cache_timestamps[addr]
            
    def _check_address_cache(self, addresses: List[str]) -> Dict[str, Dict[str, Any]]:
        """Check cache for addresses"""
        cached_results = {}
        for addr in addresses:
            if addr in self._label_cache and self._is_cache_valid(addr):
                cached_results[addr] = self._label_cache[addr]
        return cached_results
    
    def get_addresses_labels(self, addresses: List[str], chain_id: int = 1) -> List[Dict[str, Any]]:
        """Get labels for a list of addresses"""
        try:
            # Input validation
            if not addresses:
                # logger.info("Empty address list provided")
                return []
            
            # Clean and validate addresses
            cleaned_addresses = [addr.lower().strip() for addr in addresses if addr]
            if not cleaned_addresses:
                # logger.info("No valid addresses after cleaning")
                return []
            
            # Log the query parameters
            # logger.info(f"Querying labels for {len(cleaned_addresses)} addresses")
            
            # Construct query using ANY for better performance with arrays
            query = """
            SELECT 
                mca.address,
                me.entity,
                me.category AS type,
                mca.name_tag,
                mca.entity,
                mca.labels,
                mca.is_ca,
                mca.is_seed
            FROM multi_chain_addresses mca
            LEFT JOIN multi_entity me ON mca.entity = me.entity
            WHERE mca.chain_id = %(chain_id)s AND mca.address = ANY(%(addresses)s)
            """
            
            # Execute query with address list as a single parameter
            try:
                results = self.db_client.execute_query(query, {"chain_id": 0 if chain_id == 1 else chain_id, "addresses": cleaned_addresses})
                processed_results = []
                for row in results:
                    type_str = row['type'] if row['type'] is not None else ''
                    info = {
                        'address': row['address'],
                        'entity': row['entity'],
                        'type': type_str,
                        'name_tag': row['name_tag'] or '',
                        'labels': row['labels'].split(',') if row['labels'] else [],
                        'is_contract': bool(row['is_ca']),
                        'is_seed': bool(row['is_seed']),
                        'is_cex': 'CEX' in type_str.upper() or 'EXCHANGE' in type_str.upper(),
                    }
                    processed_results.append(info)
                return processed_results
            except Exception as e:
                logger.error(f"Error querying labels: {str(e)}")
                raise
                
        except Exception as e:
            logger.error(f"Error in get_addresses_labels: {str(e)}")
            raise
            
    def get_address_label(self, address: str, chain_id: int = 1) -> Optional[Dict[str, Any]]:
        """Query labels for a single address"""
        if not self._validate_address(address):
            raise ValueError(f"Invalid Ethereum address: {address}")
            
        # Check cache first
        if address in self._label_cache and self._is_cache_valid(address):
            return self._label_cache[address]
            
        results = self.get_addresses_labels([address])
        return results[0] if results else None
        
    def get_addresses_by_label(self, label: str, chain_id: int = 1) -> List[Dict[str, Any]]:
        """Find addresses by label"""
        if not label or not isinstance(label, str):
            raise ValueError("Label must be a non-empty string")
            
        query = """
            SELECT 
                mca.address,
                mca.name_tag,
                mca.labels,
                mca.is_ca,
                mca.is_seed,
                me.category AS type
            FROM multi_chain_addresses mca
            LEFT JOIN multi_entity me ON mca.entity = me.entity
            WHERE mca.chain_id = %(chain_id)s 
            AND mca.labels ILIKE %(label_pattern)s
        """
        
        try:
            with self.db_client as client:
                results = client.execute_query(
                    query,
                    parameters={
                        "chain_id": 0 if chain_id == 1 else chain_id,
                        "label_pattern": f"%{label}%"
                    }
                )
                address_info = []
                for row in results:
                    type_str = row['type'] if row['type'] is not None else ''
                    info = {
                        'address': row['address'],
                        'name_tag': row['name_tag'] or '',
                        'labels': row['labels'].split(',') if row['labels'] else [],
                        'is_contract': bool(row['is_ca']),
                        'is_seed': bool(row['is_seed']),
                        'type': type_str,
                        'is_cex': 'CEX' in type_str.upper() or 'EXCHANGE' in type_str.upper(),
                    }
                    self._update_cache(row['address'], info)
                    address_info.append(info)
                return address_info
        except Exception as e:
            logger.error(f"Database error getting addresses by label: {str(e)}")
            raise RuntimeError(f"Failed to query database: {str(e)}")
            
    def get_addresses_by_type(self, type_category: str, chain_id: int = 1) -> List[Dict[str, Any]]:
        """Find addresses by type"""
        if not type_category or not isinstance(type_category, str):
            raise ValueError("Type category must be a non-empty string")
            
        query = """
            SELECT 
                mca.address,
                mca.name_tag,
                mca.labels,
                mca.is_ca,
                mca.is_seed,
                me.category AS type
            FROM multi_chain_addresses mca
            LEFT JOIN multi_entity me ON mca.entity = me.entity
            WHERE mca.chain_id = %(chain_id)s 
            AND me.category ILIKE %(type_pattern)s
        """
        
        try:
            with self.db_client as client:
                results = client.execute_query(
                    query,
                    parameters={
                        "chain_id": chain_id,
                        "type_pattern": f"%{type_category}%"
                    }
                )
                address_info = []
                for row in results:
                    type_str = row['type'] if row['type'] is not None else ''
                    info = {
                        'address': row['address'],
                        'name_tag': row['name_tag'] or '',
                        'labels': row['labels'].split(',') if row['labels'] else [],
                        'is_contract': bool(row['is_ca']),
                        'is_seed': bool(row['is_seed']),
                        'type': type_str,
                        'is_cex': 'CEX' in type_str.upper() or 'EXCHANGE' in type_str.upper(),
                    }
                    self._update_cache(row['address'], info)
                    address_info.append(info)
                return address_info
        except Exception as e:
            logger.error(f"Database error getting addresses by type: {str(e)}")
            raise RuntimeError(f"Failed to query database: {str(e)}")