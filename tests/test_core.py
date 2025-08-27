"""
Test core functionality - converted from test_basic.py
"""
import pytest
import tempfile
from pathlib import Path
import sys

# Add parent directory to path to import modules
sys.path.insert(0, str(Path(__file__).parent.parent))

from utils.config import ConfigManager
from utils.credentials import CredentialManager
from utils.cache import CacheManager
from utils.exceptions import ConfigurationError, AuthenticationError
from api.client import Rapid7Client


class TestConfigManager:
    def test_config_defaults(self):
        """Test config manager initializes with correct defaults"""
        with tempfile.NamedTemporaryFile(delete=True) as f:
            config_path = f.name
        # file is now deleted, giving us a clean path
        
        config = ConfigManager(config_path)
        assert config.get('region') == 'au'
        assert config.get('cache_enabled') is True
        
        # Clean up
        Path(config_path).unlink(missing_ok=True)

    def test_config_set_and_save(self):
        """Test setting values and persistence"""
        with tempfile.NamedTemporaryFile(delete=True) as f:
            config_path = f.name
            
        try:
            config = ConfigManager(config_path)
            config.set('region', 'eu')
            assert config.get('region') == 'eu'
            
            # Test save/loading
            config.save_config()
            config2 = ConfigManager(config_path)
            assert config2.get('region') == 'eu'
        finally:
            Path(config_path).unlink(missing_ok=True)

    def test_config_validation(self):
        """Test config validation"""
        with tempfile.NamedTemporaryFile(delete=True) as f:
            config_path = f.name
            
        try:
            config = ConfigManager(config_path)
            config.validate()  # Should not raise
            
            config.set('region', 'invalid')
            with pytest.raises(ConfigurationError):
                config.validate()
        finally:
            Path(config_path).unlink(missing_ok=True)


class TestCredentialManager:
    def test_valid_api_key(self):
        """Test valid API key format"""
        valid, msg = CredentialManager.validate_api_key("a1b2c3d4-e5f6-7890-1234-567890abcdef")
        assert valid, f"Should be valid: {msg}"

    @pytest.mark.parametrize("invalid_key", [
        "",  # Empty
        "short",  # Too short
        None,  # None
        "x" * 300,  # Too long
    ])
    def test_invalid_api_keys(self, invalid_key):
        """Test invalid API key formats"""
        valid, msg = CredentialManager.validate_api_key(invalid_key)
        assert not valid, f"Should be invalid: {invalid_key}"


class TestCacheManager:
    def test_cache_operations(self):
        """Test basic cache operations"""
        with tempfile.TemporaryDirectory() as temp_dir:
            cache = CacheManager(cache_dir=temp_dir, ttl=3600)
            
            # Test basic operations
            cache.set('test', 'query1', {'result': 'data1'})
            result = cache.get('test', 'query1')
            assert result == {'result': 'data1'}
            
            # Test cache miss
            result = cache.get('test', 'nonexistent')
            assert result is None

    def test_cache_stats(self):
        """Test cache statistics"""
        with tempfile.TemporaryDirectory() as temp_dir:
            cache = CacheManager(cache_dir=temp_dir, ttl=3600)
            cache.set('test', 'query1', {'result': 'data1'})
            
            stats = cache.stats()
            assert 'size' in stats
            assert 'volume' in stats

    def test_cache_clear(self):
        """Test cache clearing"""
        with tempfile.TemporaryDirectory() as temp_dir:
            cache = CacheManager(cache_dir=temp_dir, ttl=3600)
            cache.set('test', 'query1', {'result': 'data1'})
            
            cache.clear()
            result = cache.get('test', 'query1')
            assert result is None


class TestAPIClient:
    def test_invalid_api_key_raises_error(self):
        """Test invalid API keys raise AuthenticationError"""
        with pytest.raises(AuthenticationError):
            Rapid7Client("")
            
        with pytest.raises(AuthenticationError):
            Rapid7Client("short")

    def test_valid_initialization(self):
        """Test valid client initialization"""
        client = Rapid7Client("a1b2c3d4-e5f6-7890-1234-567890abcdef", region='eu')
        assert client.api_key == "a1b2c3d4-e5f6-7890-1234-567890abcdef"
        assert client.region == 'eu'

    def test_base_url_generation(self):
        """Test base URL generation for different regions"""
        client = Rapid7Client("a1b2c3d4-e5f6-7890-1234-567890abcdef", region='eu')
        url = client.get_base_url('idr')
        assert 'eu.api.insight.rapid7.com' in url