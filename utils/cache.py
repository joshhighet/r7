import hashlib
import json
import platform
from pathlib import Path
from diskcache import Cache
from .exceptions import ConfigurationError
class CacheManager:
    def __init__(self, cache_dir=None, ttl=3600, max_size=1000):
        if cache_dir:
            self.cache_dir = Path(cache_dir)
        else:
            # Use platform-appropriate cache directory
            if platform.system() == 'Windows':
                # Use AppData\Local on Windows
                self.cache_dir = Path.home() / 'AppData' / 'Local' / 'rapid7-cli' / 'cache'
            else:
                # Unix-like systems (Linux, macOS)
                self.cache_dir = Path.home() / '.cache' / 'rapid7-cli'
        self.ttl = ttl
        self.max_size = max_size  # Maximum number of cache entries
        self.cache = None
    def _ensure_cache(self):
        """Initialize cache if not already done"""
        if self.cache is None:
            try:
                self.cache_dir.mkdir(parents=True, exist_ok=True)
                # Set cache size limit to prevent unlimited growth
                self.cache = Cache(str(self.cache_dir), size_limit=50*1024*1024)  # 50MB limit
            except Exception as e:
                raise ConfigurationError(f"Failed to initialize cache: {e}")
                
    def cleanup_expired(self):
        """Clean up expired cache entries"""
        if self.cache is not None:
            try:
                self.cache.expire()
                return True
            except Exception:
                return False
        return False
    def _generate_key(self, query_type, query, **kwargs):
        """Generate a unique cache key for the query"""
        key_data = {
            'type': query_type,
            'query': query,
            **kwargs
        }
        key_string = json.dumps(key_data, sort_keys=True)
        return hashlib.md5(key_string.encode()).hexdigest()
    def get(self, query_type, query, **kwargs):
        """Get cached result if exists and not expired"""
        self._ensure_cache()
        key = self._generate_key(query_type, query, **kwargs)
        return self.cache.get(key)
    def set(self, query_type, query, result, **kwargs):
        """Cache query result with TTL"""
        self._ensure_cache()
        key = self._generate_key(query_type, query, **kwargs)
        self.cache.set(key, result, expire=self.ttl)
    def clear(self):
        """Clear all cached results"""
        self._ensure_cache()
        self.cache.clear()
    def stats(self):
        """Get cache statistics"""
        self._ensure_cache()
        return {
            'size': len(self.cache),
            'volume': self.cache.volume()
        }
    
    def close(self):
        """Close the cache connection to release file locks"""
        if self.cache is not None:
            try:
                self.cache.close()
                self.cache = None
            except Exception:
                pass