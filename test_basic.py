#!/usr/bin/env python3
"""
basic test suite for the r7 CLI
"""
import sys
import tempfile
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))

from utils.config import ConfigManager
from utils.credentials import CredentialManager
from utils.cache import CacheManager
from utils.exceptions import *
from api.client import Rapid7Client

def test_config_manager():
    print("🧪 testing configmanager...")
    # test with temporary config file path (doesn't exist initially)
    with tempfile.NamedTemporaryFile(delete=True) as f:
        config_path = f.name
    # file is now deleted, giving us a clean path
    try:
        # test initialization with defaults
        config = ConfigManager(config_path)
        assert config.get('region') == 'us'
        assert config.get('cache_enabled') == True
        # test setting values
        config.set('region', 'eu')
        assert config.get('region') == 'eu'
        # test validation
        config.validate()  # Should not raise
        # test save/loading
        config.save_config()
        config2 = ConfigManager(config_path)
        assert config2.get('region') == 'eu'
        config.set('region', 'invalid')
        try:
            config.validate()
            assert False, "should have raised ConfigurationError"
        except ConfigurationError:
            pass  # Expected
        print("✅ configmanager tests passed")        
    finally:
        Path(config_path).unlink(missing_ok=True)

def test_credential_validation():
    print("🧪 testing key validation...")
    # test valid key format
    valid, msg = CredentialManager.validate_api_key("a1b2c3d4-e5f6-7890-1234-567890abcdef")
    assert valid, f"Should be valid: {msg}"
    # test invalid keys
    invalid_keys = [
        "",  # Empty
        "short",  # Too short
        None,  # None
        "x" * 300,  # Too long
    ]
    for key in invalid_keys:
        valid, msg = CredentialManager.validate_api_key(key)
        assert not valid, f"Should be invalid: {key}"
    print("✅ credential validation tests passed")

def test_cache_manager():
    print("🧪 testing cache manager...")
    with tempfile.TemporaryDirectory() as temp_dir:
        cache = CacheManager(cache_dir=temp_dir, ttl=3600)
        # test basic operations
        cache.set('test', 'query1', {'result': 'data1'})
        result = cache.get('test', 'query1')
        assert result == {'result': 'data1'}
        # test cache miss
        result = cache.get('test', 'nonexistent')
        assert result is None
        # test stats
        stats = cache.stats()
        assert 'size' in stats
        assert 'volume' in stats
        # test clear
        cache.clear()
        result = cache.get('test', 'query1')
        assert result is None
        print("✅ CacheManager tests passed")


def test_api_client_validation():
    print("🧪 API client validation...")
    # test invalid API key
    try:
        client = Rapid7Client("")
        assert False, "Should have raised AuthenticationError"
    except AuthenticationError:
        pass  # Expected    
    try:
        client = Rapid7Client("short")
        assert False, "Should have raised AuthenticationError"
    except AuthenticationError:
        pass  # Expected
    # test valid initialization (no network calls)
    client = Rapid7Client("a1b2c3d4-e5f6-7890-1234-567890abcdef", region='eu')
    assert client.api_key == "a1b2c3d4-e5f6-7890-1234-567890abcdef"
    assert client.region == 'eu'
    # test base URL generation
    url = client.get_base_url('idr')
    assert 'eu.api.insight.rapid7.com' in url
    
    print("✅ API client validation passed")


def run_all_tests():
    print("🚀 running basic test suite...\n")
    try:
        test_config_manager()
        test_credential_validation()
        test_cache_manager()
        test_api_client_validation()        
        print("\n🎉 all tests passed!")
        return True
        
    except Exception as e:
        print(f"\n❌ test failed: {e}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == '__main__':
    success = run_all_tests()
    sys.exit(0 if success else 1)
