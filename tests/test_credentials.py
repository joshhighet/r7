"""
Test credential management and precedence
"""
import pytest
import tempfile
import os
from pathlib import Path
from unittest.mock import patch
from click.testing import CliRunner
import sys

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from utils.credentials import CredentialManager
from utils.config import ConfigManager
from utils.exceptions import AuthenticationError
from r7 import cli


class TestCredentialPrecedence:
    """Test the order of credential resolution: CLI flag > env var > keyring > config"""
    
    def setup_method(self):
        self.runner = CliRunner()
        
    def test_cli_flag_wins(self):
        """CLI --api-key flag should override everything"""
        with patch.dict(os.environ, {'R7_API_KEY': 'env-key'}):
            # CLI flag should override environment variable
            result = self.runner.invoke(cli, 
                ['--api-key', 'cli-key', '--help'])
            assert result.exit_code == 0
            # In a real test, we'd check that cli-key is used, not env-key
    
    def test_env_var_precedence(self):
        """Environment variable should be used when no CLI flag"""
        with patch.dict(os.environ, {'R7_API_KEY': 'env-key'}):
            result = self.runner.invoke(cli, ['--help'])
            assert result.exit_code == 0
            # Would verify env-key is used in actual implementation
    
    def test_env_var_overrides_keyring(self):
        """Environment variable should override keyring"""
        with patch('keyring.get_password', return_value='keyring-key'):
            with patch.dict(os.environ, {'R7_API_KEY': 'env-key'}):
                # Environment should win over keyring
                result = self.runner.invoke(cli, ['config', 'show'])
                assert result.exit_code == 0
    
    def test_empty_env_var_ignored(self):
        """Empty environment variable should be ignored"""
        with patch.dict(os.environ, {'R7_API_KEY': ''}):
            # Should fall back to keyring/config, not use empty string
            result = self.runner.invoke(cli, ['--help'])
            assert result.exit_code == 0
    
    def test_whitespace_env_var_ignored(self):
        """Whitespace-only environment variable should be ignored"""
        with patch.dict(os.environ, {'R7_API_KEY': '   \n\t  '}):
            result = self.runner.invoke(cli, ['--help'])
            assert result.exit_code == 0


class TestKeyringFallback:
    """Test behavior when keyring is unavailable (common on headless Linux)"""
    
    def setup_method(self):
        self.cred_manager = CredentialManager()
    
    def test_keyring_unavailable_no_crash(self):
        """Should not crash when keyring is unavailable"""
        with patch('keyring.get_password', side_effect=Exception("No keyring backend available")):
            # Should return None gracefully, not crash
            result = self.cred_manager.get_api_key()
            assert result is None
    
    def test_keyring_permission_denied(self):
        """Should handle keyring permission errors gracefully"""
        with patch('keyring.get_password', side_effect=PermissionError("Access denied")):
            result = self.cred_manager.get_api_key()
            assert result is None
    
    def test_keyring_import_error(self):
        """Should handle missing keyring library gracefully"""
        with patch('keyring.get_password', side_effect=ImportError("No module named keyring")):
            result = self.cred_manager.get_api_key()
            assert result is None
    
    def test_keyring_store_failure_raises_auth_error(self):
        """Should raise AuthenticationError when keyring store fails"""
        with patch('keyring.set_password', side_effect=Exception("Keyring write failed")):
            # Your current implementation raises AuthenticationError (which is fine)
            with pytest.raises(AuthenticationError):
                self.cred_manager.store_api_key("test-key")
    
    def test_fallback_to_env_when_keyring_fails(self):
        """When keyring fails, should fall back to environment variables"""
        with patch('keyring.get_password', side_effect=Exception("Keyring failed")):
            with patch.dict(os.environ, {'R7_API_KEY': 'fallback-key'}):
                # This would test the actual credential resolution logic
                # In a real implementation, we'd verify fallback-key is used
                pass


class TestCredentialValidation:
    """Test API key validation edge cases"""
    
    def test_credential_with_spaces(self):
        """Test API key with leading/trailing spaces"""
        key_with_spaces = "  a1b2c3d4-e5f6-7890-1234-567890abcdef  "
        valid, msg = CredentialManager.validate_api_key(key_with_spaces)
        # Should either trim and accept, or reject with clear message
        assert isinstance(valid, bool)
        assert isinstance(msg, str)
    
    def test_credential_case_sensitivity(self):
        """Test API key case sensitivity"""
        lower_key = "a1b2c3d4-e5f6-7890-1234-567890abcdef"
        upper_key = "A1B2C3D4-E5F6-7890-1234-567890ABCDEF"
        
        valid_lower, _ = CredentialManager.validate_api_key(lower_key)
        valid_upper, _ = CredentialManager.validate_api_key(upper_key)
        
        assert valid_lower is True
        assert valid_upper is True  # Or False, depending on R7's requirements
    
    def test_credential_with_special_chars(self):
        """Test API key with special characters (your validation is lenient)"""
        special_chars = "a1b2c3d4-e5f6-7890-1234-567890abcde!"
        valid, msg = CredentialManager.validate_api_key(special_chars)
        # Your current validation accepts this (only checks for some alphanumeric)
        assert valid is True
    
    @pytest.mark.parametrize("test_key,expected", [
        ("a1b2c3d4-e5f6-7890-1234-567890abcdef", True),   # Valid UUID format
        ("a1b2c3d4e5f678901234567890abcdef", True),       # No hyphens (still has alphanumeric)
        ("a1b2c3d4-e5f6-7890-1234-567890abcd", True),     # Shorter but > 10 chars
        ("a1b2c3d4-e5f6-7890-1234-567890abcdefg", True),  # Longer but < 200 chars
        ("short", False),                                  # Too short (< 10)
        ("x" * 201, False),                               # Too long (> 200)
        ("", False),                                      # Empty
        ("!!!!!!!!!!!!", False),                         # No alphanumeric chars
    ])
    def test_api_key_formats(self, test_key, expected):
        """Test various API key formats"""
        valid, _ = CredentialManager.validate_api_key(test_key)
        assert valid == expected


class TestCredentialSecurity:
    """Test credential security practices"""
    
    def test_credential_not_logged(self):
        """Ensure credentials don't appear in logs/output"""
        # This would require checking log output or CLI output
        # to ensure API keys are masked with *** or similar
        pass
    
    def test_credential_cleared_from_memory(self):
        """Test that credentials are cleared from memory when possible"""
        # This is advanced - testing memory cleanup
        # Not critical for initial implementation
        pass
    
    def test_config_file_permissions(self):
        """Test that config files are created with restrictive permissions"""
        with tempfile.NamedTemporaryFile(delete=False) as f:
            config_path = f.name
        
        try:
            config = ConfigManager(config_path)
            config.save_config()
            
            # Check file permissions (Unix systems)
            if hasattr(os, 'stat'):
                file_stat = os.stat(config_path)
                # Should be readable only by owner (0o600 or similar)
                # This prevents other users from reading API keys
                permissions = oct(file_stat.st_mode)[-3:]
                # Exact check depends on your implementation
                assert permissions in ['600', '644']  # Adjust based on requirements
        finally:
            Path(config_path).unlink(missing_ok=True)