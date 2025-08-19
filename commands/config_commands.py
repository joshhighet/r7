import click
import json
from utils.config import ConfigManager
from utils.exceptions import ConfigurationError
from commands.credential_commands import cred_group as _cred_group

# Map CLI option names to config keys
CONFIG_KEY_MAPPING = {
    'output': 'default_output',
    'cache': 'cache_enabled',
    'cache_ttl': 'cache_ttl',
    'max_pages': 'max_result_pages',
    'region': 'region',
    'verbose': 'verbose',
    'vm_console_url': 'vm_console_url',
    'vm_verify_ssl': 'vm_verify_ssl',
    'vm_tenant_prefix': 'vm_tenant_prefix',
    'smart_columns': 'smart_columns_enabled',
    'smart_columns_max': 'smart_columns_max',
    'max_chars': 'max_chars'
}

@click.group(name='config')
def config_group():
    """manage local configuration"""
    pass

# Attach credential sub-commands under config
config_group.add_command(_cred_group, name='cred')


@config_group.command()
def show():
    """Show current configuration"""
    try:
        config_manager = ConfigManager()
        config_manager.validate()
        click.echo(json.dumps(config_manager.config, indent=2))
    except ConfigurationError as e:
        click.echo(f"❌ Configuration error: {e}", err=True)

@config_group.command()
@click.option('--region', type=click.Choice(['us', 'eu', 'ca', 'ap', 'au']), help='Default region')
@click.option('--output', type=click.Choice(['simple', 'table', 'json']), help='Default output format')
@click.option('--max-pages', type=int, help='Default max pages for pagination')
@click.option('--cache/--no-cache', default=None, help='Enable/disable caching')
@click.option('--cache-ttl', type=int, help='Cache TTL in seconds')
@click.option('--verbose/--no-verbose', default=None, help='Enable verbose logging')
@click.option('--vm-console-url', help='InsightVM console API base URL, e.g., https://host:3780/api/3')
@click.option('--vm-verify-ssl/--no-vm-verify-ssl', default=None, help='Verify SSL when talking to console')
@click.option('--vm-tenant-prefix', help='VM tenant prefix for shortening asset IDs, e.g., "fd7eb9b8-99d4-4a63-b4e1-"')
@click.option('--smart-columns/--no-smart-columns', default=None, help='Enable/disable smart columns by default')
@click.option('--smart-columns-max', type=int, help='Default maximum number of smart columns')
@click.option('--max-chars', type=int, help='Maximum characters to display per log entry')

def set(**kwargs):
    """Set configuration values"""
    try:
        config_manager = ConfigManager()
        updates = {}
        for cli_key, value in kwargs.items():
            if value is not None:
                config_key = CONFIG_KEY_MAPPING.get(cli_key, cli_key)
                updates[config_key] = value
        
        if not updates:
            click.echo("❌ No configuration values provided to set")
            click.echo("Use --help to see available options")
            return
        
        config_manager.update(updates)
        config_manager.validate()
        config_manager.save_config()
        click.echo("✅ Configuration updated successfully")
        click.echo("Updated values:")
        for key, value in updates.items():
            click.echo(f" {key}: {value}")
    except ConfigurationError as e:
        click.echo(f"❌ Configuration error: {e}", err=True)

@config_group.command()
def reset():
    """Reset configuration to defaults"""
    if click.confirm("This will reset all configuration to defaults. Continue?"):
        try:
            config_manager = ConfigManager()
            config_manager.reset_to_defaults()
            config_manager.save_config()
            click.echo("✅ Configuration reset to defaults")
        except ConfigurationError as e:
            click.echo(f"❌ Error resetting configuration: {e}", err=True)

@config_group.command()
def validate():
    """Validate current configuration"""
    try:
        config_manager = ConfigManager()
        config_manager.validate()
        click.echo("✅ Configuration is valid")
    except ConfigurationError as e:
        click.echo(f"❌ Configuration error: {e}", err=True)

@config_group.command()
@click.pass_context
def test(ctx):
    """Test API connectivity and authentication"""
    try:
        from api.client import Rapid7Client
        from utils.credentials import CredentialManager
        
        config_manager = ConfigManager()
        config_manager.validate()
        
        api_key = CredentialManager.get_api_key()
        if not api_key:
            click.echo("❌ No API key found. Use 'config cred store' to set up authentication.", err=True)
            return
            
        region = config_manager.get('region', 'us')
        click.echo(f"🔍 Testing connection to {region.upper()} region...")
        
        client = Rapid7Client(api_key, region)
        result = client.test_connection()
        
        if result['success']:
            click.echo(f"✅ {result['message']}")
            if 'organizations_count' in result:
                click.echo(f"📊 Found {result['organizations_count']} organization(s)")
        else:
            click.echo(f"❌ {result['message']}", err=True)
            
    except Exception as e:
        click.echo(f"❌ Connection test failed: {e}", err=True)

@config_group.command()
def cache():
    """Manage cache (show stats, cleanup)"""
    try:
        from utils.cache import CacheManager
        
        config_manager = ConfigManager()
        if not config_manager.get('cache_enabled'):
            click.echo("⚠️  Cache is currently disabled")
            click.echo("Enable with: r7 config set --cache")
            return
            
        cache_manager = CacheManager(ttl=config_manager.get('cache_ttl'))
        stats = cache_manager.stats()
        
        click.echo("📊 Cache Statistics:")
        click.echo(f"  Entries: {stats['size']}")
        click.echo(f"  Volume: {stats['volume']} bytes")
        
        if click.confirm("Clear cache?"):
            cache_manager.clear()
            click.echo("✅ Cache cleared")
            
    except Exception as e:
        click.echo(f"❌ Cache operation failed: {e}", err=True)