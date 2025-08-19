"""
Centralized CLI utilities and base classes for command modules.
This module reduces code duplication across all command modules.
"""
import json
import sys
import logging
from datetime import datetime
from functools import wraps
from typing import Dict, Any, Optional, Union, List

import click
from rich.console import Console
from rich.table import Table
from rich.panel import Panel

from .config import ConfigManager
from .cache import CacheManager
from .credentials import CredentialManager
from .exceptions import *
from api.client import Rapid7Client

logger = logging.getLogger(__name__)
console = Console()


class OutputFormatter:
    """Centralized output formatting for all command responses."""
    
    @staticmethod
    def format_timestamp(timestamp: Union[str, int, None]) -> str:
        """Convert various timestamp formats to readable format."""
        if not timestamp:
            return ''
        try:
            if isinstance(timestamp, int):
                # Unix timestamp in milliseconds
                dt = datetime.fromtimestamp(timestamp / 1000)
                return dt.strftime('%Y-%m-%d %H:%M:%S')
            elif isinstance(timestamp, str) and 'T' in timestamp:
                # ISO format
                dt = datetime.fromisoformat(timestamp.replace('Z', '+00:00'))
                return dt.strftime('%Y-%m-%d %H:%M:%S')
            return str(timestamp)
        except (ValueError, TypeError):
            return str(timestamp)

    @staticmethod
    def format_bytes(bytes_value: Union[int, float, None]) -> str:
        """Format bytes into human readable format."""
        if not bytes_value:
            return "0 B"
        
        for unit in ['B', 'KB', 'MB', 'GB', 'TB', 'PB']:
            if bytes_value < 1024.0:
                return f"{bytes_value:.1f} {unit}"
            bytes_value /= 1024.0
        return f"{bytes_value:.1f} EB"

    @staticmethod
    def should_use_json_output(output_format: Optional[str], config_default: str) -> bool:
        """Determine if we should use JSON output based on pipe detection and user preference."""
        if output_format:
            return output_format == 'json'
        if not sys.stdout.isatty():
            return True
        return config_default == 'json'

    @staticmethod
    def output_data(data: Any, output_format: Optional[str], config_manager: ConfigManager,
                   table_formatter: Optional[callable] = None, simple_formatter: Optional[callable] = None) -> None:
        """
        Universal output formatter for all command responses.
        
        Args:
            data: The data to output
            output_format: User-specified output format ('json', 'table', 'simple')
            config_manager: Configuration manager for defaults
            table_formatter: Function to format data as rich table
            simple_formatter: Function to format data as simple text
        """
        use_json = OutputFormatter.should_use_json_output(output_format, config_manager.get('default_output'))
        
        if use_json:
            click.echo(json.dumps(data, indent=2))
        elif output_format == 'table' and table_formatter:
            table_formatter(data)
        elif output_format == 'simple' and simple_formatter:
            simple_formatter(data)
        elif table_formatter:  # Default to table if available
            table_formatter(data)
        else:
            # Fallback to JSON
            click.echo(json.dumps(data, indent=2))

    @staticmethod
    def create_standard_table(title: str, columns: List[Dict[str, str]]) -> Table:
        """Create a standardized Rich table with common styling."""
        table = Table(title=title)
        for col in columns:
            table.add_column(col['name'], style=col.get('style', 'white'), width=col.get('width'))
        return table

    @staticmethod
    def display_cached_message():
        """Display standard cached result message."""
        console.print("📋 Using cached result", style="dim")

    @staticmethod
    def display_error(error: Exception, context: str = ""):
        """Display standardized error message."""
        error_msg = f"❌ {context}{': ' if context else ''}{str(error)}"
        click.echo(error_msg, err=True)


class ClientManager:
    """Centralized client management and configuration."""
    
    _instance = None
    _clients = {}
    
    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(ClientManager, cls).__new__(cls)
        return cls._instance
    
    def get_client_and_config(self, ctx, api_key: Optional[str] = None, 
                             cache_namespace: str = 'default') -> tuple[Rapid7Client, ConfigManager]:
        """
        Get configured API client and config manager with caching.
        
        Args:
            ctx: Click context
            api_key: Optional API key override
            cache_namespace: Cache namespace for this client type
            
        Returns:
            Tuple of (client, config_manager)
        """
        try:
            config_manager = ConfigManager()
            config_manager.validate()
            
            final_api_key = CredentialManager.get_api_key(api_key or ctx.obj.get('api_key'))
            if not final_api_key:
                raise AuthenticationError(
                    "No API key found. Use --api-key, set R7_API_KEY environment variable, "
                    "or store in keychain with 'r7 config cred store --api-key YOUR_KEY'"
                )
            
            # Create cache key for client reuse
            region = ctx.obj.get('region') or config_manager.get('region')
            client_key = f"{final_api_key[:8]}_{region}_{cache_namespace}"
            
            if client_key not in self._clients:
                cache_manager = None
                if config_manager.get('cache_enabled'):
                    cache_manager = CacheManager(ttl=config_manager.get('cache_ttl'))
                
                self._clients[client_key] = Rapid7Client(final_api_key, region, cache_manager)
            
            return self._clients[client_key], config_manager
            
        except (ConfigurationError, AuthenticationError) as e:
            OutputFormatter.display_error(e)
            ctx.exit(1)


class CacheableCommand:
    """Base class for commands that support caching."""
    
    def __init__(self, cache_namespace: str):
        self.cache_namespace = cache_namespace
        self.client_manager = ClientManager()
    
    def get_cached_data(self, client: Rapid7Client, cache_key: str, 
                       no_cache: bool, data_fetcher: callable) -> Any:
        """
        Generic cached data retrieval pattern.
        
        Args:
            client: Rapid7Client instance
            cache_key: Cache key for this data
            no_cache: Whether to bypass cache
            data_fetcher: Function that fetches the data
            
        Returns:
            The requested data (cached or fresh)
        """
        cached_result = None
        if client.cache_manager and not no_cache:
            cached_result = client.cache_manager.get(self.cache_namespace, cache_key)
            if cached_result:
                OutputFormatter.display_cached_message()
        
        if not cached_result:
            data = data_fetcher()
            if client.cache_manager and not no_cache:
                client.cache_manager.set(self.cache_namespace, cache_key, data)
            return data
        
        return cached_result


# Common Click decorators for reuse
def common_output_options(f):
    """Add standard output format options to a command."""
    f = click.option('--output', type=click.Choice(['simple', 'table', 'json']), 
                    help='Output format')(f)
    f = click.option('--no-cache', is_flag=True, 
                    help='Disable caching for this query')(f)
    return f


def error_handler(f):
    """Decorator to add standard error handling to commands."""
    @wraps(f)
    def wrapper(*args, **kwargs):
        try:
            return f(*args, **kwargs)
        except (APIError, ConfigurationError, AuthenticationError) as e:
            OutputFormatter.display_error(e)
            raise click.Abort()
        except Exception as e:
            OutputFormatter.display_error(e, "Unexpected error")
            raise click.Abort()
    return wrapper


class BaseListCommand(CacheableCommand):
    """Base class for list commands with standard patterns."""
    
    def __init__(self, cache_namespace: str, list_method: str):
        super().__init__(cache_namespace)
        self.list_method = list_method
    
    @error_handler
    def execute(self, ctx, output: Optional[str], no_cache: bool, 
                cache_key: str, table_formatter: callable, 
                simple_formatter: callable, **kwargs):
        """Execute a standard list command."""
        client, config_manager = self.client_manager.get_client_and_config(
            ctx, cache_namespace=self.cache_namespace)
        
        # Get the method from client
        data_fetcher = getattr(client, self.list_method)
        
        # Get data with caching
        data = self.get_cached_data(client, cache_key, no_cache, 
                                   lambda: data_fetcher(**kwargs))
        
        # Output data
        OutputFormatter.output_data(data, output, config_manager, 
                                  table_formatter, simple_formatter)


class BaseGetCommand(CacheableCommand):
    """Base class for get commands with standard patterns."""
    
    def __init__(self, cache_namespace: str, get_method: str):
        super().__init__(cache_namespace)
        self.get_method = get_method
    
    @error_handler
    def execute(self, ctx, item_id: str, output: Optional[str], 
                no_cache: bool, table_formatter: callable, 
                simple_formatter: callable, **kwargs):
        """Execute a standard get command."""
        client, config_manager = self.client_manager.get_client_and_config(
            ctx, cache_namespace=self.cache_namespace)
        
        cache_key = f"{self.get_method}_{item_id}"
        
        # Get the method from client
        data_fetcher = getattr(client, self.get_method)
        
        # Get data with caching
        data = self.get_cached_data(client, cache_key, no_cache, 
                                   lambda: data_fetcher(item_id, **kwargs))
        
        # Output data
        OutputFormatter.output_data(data, output, config_manager, 
                                  table_formatter, simple_formatter)


# Table formatters for common data types
class TableFormatters:
    """Collection of standard table formatters for common data types."""
    
    @staticmethod
    def format_api_keys_table(data: Dict[str, Any]):
        """Format API keys data as a table."""
        if 'data' in data and data['data']:
            table = OutputFormatter.create_standard_table("API Keys", [
                {'name': 'ID', 'style': 'cyan'},
                {'name': 'Name', 'style': 'white'},
                {'name': 'Type', 'style': 'green'},
                {'name': 'Generated On', 'style': 'yellow'}
            ])
            
            for key in data['data']:
                table.add_row(
                    key.get('id', ''),
                    key.get('name', ''),
                    key.get('type', ''),
                    OutputFormatter.format_timestamp(key.get('generated_on', ''))
                )
            console.print(table)
        else:
            console.print("No API keys found", style="yellow")
    
    @staticmethod
    def format_users_table(data: Dict[str, Any]):
        """Format users data as a table."""
        if 'data' in data and data['data']:
            table = OutputFormatter.create_standard_table("Users", [
                {'name': 'ID', 'style': 'cyan'},
                {'name': 'Email', 'style': 'white'},
                {'name': 'Name', 'style': 'yellow'},
                {'name': 'Status', 'style': 'green'}
            ])
            
            for user in data['data']:
                table.add_row(
                    user.get('id', ''),
                    user.get('email', ''),
                    f"{user.get('first_name', '')} {user.get('last_name', '')}".strip(),
                    user.get('status', '')
                )
            console.print(table)
        else:
            console.print("No users found", style="yellow")
    
    @staticmethod
    def format_organizations_table(data: Dict[str, Any]):
        """Format organizations data as a table."""
        if 'data' in data and data['data']:
            table = OutputFormatter.create_standard_table("Organizations", [
                {'name': 'ID', 'style': 'cyan'},
                {'name': 'Name', 'style': 'white'},
                {'name': 'Type', 'style': 'yellow'}
            ])
            
            for org in data['data']:
                table.add_row(
                    org.get('id', ''),
                    org.get('name', ''),
                    org.get('type', '')
                )
            console.print(table)
        else:
            console.print("No organizations found", style="yellow")

    @staticmethod
    def format_products_table(data: Dict[str, Any]):
        """Format products data as a table."""
        products_list = data.get('data', data) if isinstance(data, dict) and 'data' in data else data
        if products_list:
            table = OutputFormatter.create_standard_table("Products", [
                {'name': 'Token', 'style': 'cyan'},
                {'name': 'Code', 'style': 'white'},
                {'name': 'Organization', 'style': 'green'}
            ])
            
            for product in products_list:
                table.add_row(
                    product.get('product_token', ''),
                    product.get('product_code', ''),
                    product.get('organization_name', '')
                )
            console.print(table)
        else:
            console.print("No products found", style="yellow")


# Simple formatters for common data types
class SimpleFormatters:
    """Collection of simple text formatters for common data types."""
    
    @staticmethod
    def format_api_keys_simple(data: Dict[str, Any]):
        """Format API keys data as simple text."""
        if 'data' in data and data['data']:
            for key in data['data']:
                click.echo(f"{key.get('name', '')} {key.get('id', '')}")
        else:
            click.echo("No API keys found")
    
    @staticmethod
    def format_users_simple(data: Dict[str, Any]):
        """Format users data as simple text."""
        if 'data' in data and data['data']:
            for user in data['data']:
                click.echo(f"{user.get('email', '')} {user.get('id', '')}")
        else:
            click.echo("No users found")
    
    @staticmethod
    def format_organizations_simple(data: Dict[str, Any]):
        """Format organizations data as simple text."""
        if 'data' in data and data['data']:
            for org in data['data']:
                click.echo(f"{org.get('name', '')} {org.get('id', '')}")
        else:
            click.echo("No organizations found")

    @staticmethod
    def format_products_simple(data: Dict[str, Any]):
        """Format products data as simple text."""
        products_list = data.get('data', data) if isinstance(data, dict) and 'data' in data else data
        if products_list:
            for product in products_list:
                click.echo(f"{product.get('product_code', '')} {product.get('product_token', '')}")
        else:
            click.echo("No products found")
