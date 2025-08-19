import os
import keyring
import click
from .exceptions import AuthenticationError
class CredentialManager:
    SERVICE_NAME = "rapid7-cli"
    API_KEY_NAME = "api-key"
    # VM console password stored under a dedicated service namespace
    VM_SERVICE_NAME = "rapid7-cli-vm"
    VM_PASSWORD_NAME = "vm-password"
    @classmethod
    def get_api_key(cls, provided_key=None):
        """Get API key from various sources in priority order:
        1. Provided key (CLI argument)
        2. Environment variable R7_API_KEY
        3. macOS Keychain
        """
        if provided_key:
            return provided_key
        env_key = os.getenv('R7_API_KEY')
        if env_key:
            return env_key
        try:
            keychain_key = keyring.get_password(cls.SERVICE_NAME, cls.API_KEY_NAME)
            if keychain_key:
                return keychain_key
        except Exception:
            pass
        return None
    @classmethod
    def store_api_key(cls, api_key):
        """Store API key in macOS Keychain"""
        try:
            keyring.set_password(cls.SERVICE_NAME, cls.API_KEY_NAME, api_key)
            return True
        except Exception as e:
            raise AuthenticationError(f"Failed to store API key in keychain: {e}")
    @classmethod
    def delete_api_key(cls):
        """Delete API key from macOS Keychain"""
        try:
            keyring.delete_password(cls.SERVICE_NAME, cls.API_KEY_NAME)
            return True
        except keyring.errors.PasswordDeleteError:
            return False
        except Exception as e:
            raise AuthenticationError(f"Failed to delete API key from keychain: {e}")
    @classmethod
    def validate_api_key(cls, api_key):
        """Basic API key validation with detailed feedback"""
        if not api_key:
            return False, "API key is empty"
        
        api_key = api_key.strip()
        if len(api_key) < 10:
            return False, "API key appears too short (should be longer than 10 characters)"
            
        # Basic format validation - Rapid7 API keys are typically UUID-like
        if not any(c.isalnum() for c in api_key):
            return False, "API key should contain alphanumeric characters"
            
        if len(api_key) > 200:
            return False, "API key appears too long"
            
        return True, "API key format appears valid"

    # --- InsightVM Console password management ---
    @classmethod
    def store_vm_password(cls, password: str) -> bool:
        """Store VM console password in macOS Keychain"""
        if not password:
            raise AuthenticationError("Password is empty")
        try:
            keyring.set_password(cls.VM_SERVICE_NAME, cls.VM_PASSWORD_NAME, password)
            return True
        except Exception as e:
            raise AuthenticationError(f"Failed to store VM password in keychain: {e}")

    @classmethod
    def get_vm_password(cls) -> str | None:
        """Retrieve VM console password from macOS Keychain"""
        try:
            return keyring.get_password(cls.VM_SERVICE_NAME, cls.VM_PASSWORD_NAME)
        except Exception:
            return None

    @classmethod
    def delete_vm_password(cls) -> bool:
        """Delete VM console password from macOS Keychain"""
        try:
            keyring.delete_password(cls.VM_SERVICE_NAME, cls.VM_PASSWORD_NAME)
            return True
        except keyring.errors.PasswordDeleteError:
            return False
        except Exception as e:
            raise AuthenticationError(f"Failed to delete VM password from keychain: {e}")
