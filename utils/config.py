import json
from pathlib import Path
from .exceptions import ConfigurationError
class ConfigManager:
    DEFAULT_CONFIG_PATH = Path.home() / '.rapid7_config.json'
    DEFAULT_CONFIG = {
        'region': 'au',
        'default_output': 'table',
        'max_result_pages': 3,
        'query_timeout': 300,
        'cache_enabled': True,
        'cache_ttl': 3600,
        'verbose': False,
        'max_chars': 500,
    'organization_id': None,
    'vm_console_url': None,
    'vm_username': None,
    'vm_verify_ssl': True
    }
    def __init__(self, config_path=None):
        self.config_path = Path(config_path) if config_path else self.DEFAULT_CONFIG_PATH
        self.config = self._load_config()
    def _load_config(self):
        """Load configuration from file or create default"""
        if not self.config_path.exists():
            return self.DEFAULT_CONFIG.copy()
        
        # Check if file is empty
        try:
            if self.config_path.stat().st_size == 0:
                return self.DEFAULT_CONFIG.copy()
        except OSError:
            return self.DEFAULT_CONFIG.copy()
            
        try:
            with open(self.config_path, 'r') as f:
                config = json.load(f)
                merged_config = self.DEFAULT_CONFIG.copy()
                merged_config.update(config)
                return merged_config
        except (json.JSONDecodeError, IOError) as e:
            raise ConfigurationError(f"Failed to load config from {self.config_path}: {e}")
    def save_config(self):
        """Save current configuration to file"""
        try:
            self.config_path.parent.mkdir(exist_ok=True, parents=True)
            with open(self.config_path, 'w') as f:
                json.dump(self.config, f, indent=2)
        except IOError as e:
            raise ConfigurationError(f"Failed to save config to {self.config_path}: {e}")
    def get(self, key, default=None):
        """Get configuration value"""
        return self.config.get(key, default)
    def set(self, key, value):
        """Set configuration value"""
        self.config[key] = value
    def update(self, updates):
        """Update multiple configuration values"""
        self.config.update(updates)
    def reset_to_defaults(self):
        """Reset configuration to defaults"""
        self.config = self.DEFAULT_CONFIG.copy()
    def validate(self):
        """Validate configuration values"""
        valid_regions = ['us', 'eu', 'ca', 'ap', 'au']
        valid_outputs = ['simple', 'table', 'json']
        if self.config.get('region') not in valid_regions:
            raise ConfigurationError(f"Invalid region: {self.config.get('region')}. Must be one of {valid_regions}")
        if self.config.get('default_output') not in valid_outputs:
            raise ConfigurationError(f"Invalid output format: {self.config.get('default_output')}. Must be one of {valid_outputs}")
        if not isinstance(self.config.get('max_result_pages'), int) or self.config.get('max_result_pages') < 1:
            raise ConfigurationError("max_result_pages must be a positive integer")
        if not isinstance(self.config.get('query_timeout'), int) or self.config.get('query_timeout') < 30:
            raise ConfigurationError("query_timeout must be at least 30 seconds")
        if not isinstance(self.config.get('cache_ttl'), int) or self.config.get('cache_ttl') < 0:
            raise ConfigurationError("cache_ttl must be a non-negative integer")
