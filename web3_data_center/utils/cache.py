import os
import json
import time
import hashlib
import functools
from pathlib import Path
from typing import Any, Callable, Optional, TypeVar, Dict

from ..utils.logger import get_logger

logger = get_logger(__name__)

T = TypeVar('T')

def get_cache_dir() -> Path:
    """Get the cache directory path."""
    cache_dir = Path.home() / '.web3_data_center' / 'cache'
    cache_dir.mkdir(parents=True, exist_ok=True)
    return cache_dir

def make_cache_key(*args, **kwargs) -> str:
    """Create a cache key from function arguments."""
    # Convert args and kwargs to a string representation
    key_parts = [str(arg) for arg in args]
    key_parts.extend(f"{k}:{v}" for k, v in sorted(kwargs.items()))
    key_str = "|".join(key_parts)
    
    # Create a hash of the key string
    return hashlib.md5(key_str.encode()).hexdigest()

def file_cache(
    namespace: str,
    ttl: Optional[int] = None,
    max_entries: Optional[int] = 1000
):
    """
    A file-based caching decorator for async functions.
    
    Args:
        namespace: Namespace for the cache (used in filename)
        ttl: Time to live in seconds (default: None, meaning no expiration)
        max_entries: Maximum number of entries in cache file (default: 1000)
    """
    cache_dir = get_cache_dir()
    cache_file = cache_dir / f"{namespace}_cache.json"
    
    def load_cache() -> Dict[str, Any]:
        """Load cache from file."""
        try:
            if cache_file.exists():
                with open(cache_file, 'r') as f:
                    return json.load(f)
            return {}
        except Exception as e:
            logger.error(f"Error loading cache: {e}")
            return {}
    
    def save_cache(cache: Dict[str, Any]):
        """Save cache to file."""
        try:
            with open(cache_file, 'w') as f:
                json.dump(cache, f)
        except Exception as e:
            logger.error(f"Error saving cache: {e}")
    
    def clean_expired(cache: Dict[str, Any]):
        """Remove expired entries."""
        if not ttl:
            return cache
            
        current_time = time.time()
        return {
            k: v for k, v in cache.items()
            if v.get('timestamp', 0) + ttl > current_time
        }
    
    def clean_overflow(cache: Dict[str, Any]):
        """Remove oldest entries if cache exceeds max size."""
        if not max_entries or len(cache) <= max_entries:
            return cache
            
        # Sort by timestamp and keep only the newest max_entries
        sorted_items = sorted(
            cache.items(),
            key=lambda x: x[1].get('timestamp', 0),
            reverse=True
        )
        return dict(sorted_items[:max_entries])
    
    def decorator(func: Callable[..., T]) -> Callable[..., T]:
        @functools.wraps(func)
        async def wrapper(*args, **kwargs):
            # Generate cache key
            key = make_cache_key(*args, **kwargs)
            
            # Load and clean cache
            cache = load_cache()
            cache = clean_expired(cache)
            cache = clean_overflow(cache)
            
            # Check if we have a valid cached result
            if key in cache:
                return cache[key]['data']
            
            # Call the original function
            result = await func(*args, **kwargs)
            
            # Store result in cache
            cache[key] = {
                'data': result,
                'timestamp': time.time()
            }
            
            # Save updated cache
            save_cache(cache)
            
            return result
        
        # Add clear cache method
        wrapper.cache_clear = lambda: cache_file.unlink(missing_ok=True)
        
        return wrapper
    
    return decorator
