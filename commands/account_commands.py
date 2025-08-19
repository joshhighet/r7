import click
import json
import sys
from datetime import datetime
from rich.console import Console
from rich.table import Table
from api.client import Rapid7Client
from utils.config import ConfigManager
from utils.cache import CacheManager
from utils.credentials import CredentialManager
from utils.exceptions import *

console = Console()

def get_client_and_config(ctx, api_key=None):
    """Get configured API client and config manager"""
    try:
        config_manager = ConfigManager()
        config_manager.validate()
        final_api_key = CredentialManager.get_api_key(api_key)
        if not final_api_key:
            raise AuthenticationError(
                "No API key found. Use --api-key, set R7_API_KEY environment variable, "
                "or store in keychain with 'r7 config cred store --api-key YOUR_KEY'"
            )
        cache_manager = None
        if config_manager.get('cache_enabled'):
            cache_manager = CacheManager(ttl=config_manager.get('cache_ttl'))
        region = ctx.params.get('region') or config_manager.get('region')
        client = Rapid7Client(final_api_key, region, cache_manager)
        return client, config_manager
    except (ConfigurationError, AuthenticationError) as e:
        click.echo(f"❌ {e}", err=True)
        ctx.exit(1)

def should_use_json_output(output_format, config_default):
    """Determine if we should use JSON output based on pipe detection and user preference"""
    if output_format:
        return output_format == 'json'
    if not sys.stdout.isatty():
        return True
    return config_default == 'json'

def get_output_format(output, config_manager):
    """Determine the output format to use"""
    if output:
        return output
    elif not sys.stdout.isatty():
        return 'simple'
    else:
        return config_manager.get('default_output', 'simple')

def format_timestamp(timestamp):
    """Convert ISO timestamp to readable format"""
    if not timestamp:
        return ''
    try:
        # Handle ISO format timestamps
        if 'T' in timestamp:
            dt = datetime.fromisoformat(timestamp.replace('Z', '+00:00'))
            return dt.strftime('%Y-%m-%d %H:%M:%S')
        return timestamp
    except (ValueError, TypeError):
        return str(timestamp)

@click.group(name='account')
def account_group():
    """manage users, keys, roles, access etc"""
    pass

# API Keys subcommands
@account_group.group(name='keys')
def keys_group():
    """Manage API keys"""
    pass

@keys_group.command(name='list')
@click.option('--output', type=click.Choice(['simple', 'table', 'json']), help='Output format')
@click.option('--no-cache', is_flag=True, help='Disable caching for this query')
@click.pass_context
def list_keys(ctx, output, no_cache):
    """List all API keys"""
    client, config_manager = get_client_and_config(ctx)
    if output:
        use_format = output
    elif not sys.stdout.isatty():
        use_format = 'simple'
    else:
        use_format = config_manager.get('default_output', 'simple')
    
    try:
        cache_key = "api_keys"
        cached_result = None
        if client.cache_manager and not no_cache:
            cached_result = client.cache_manager.get('account_api', cache_key)
            if cached_result and use_format != 'json':
                console.print("📋 Using cached result", style="dim")
        
        if not cached_result:
            data = client.list_api_keys()
            if client.cache_manager and not no_cache:
                client.cache_manager.set('account_api', cache_key, data)
        else:
            data = cached_result
        
        if use_format == 'json':
            click.echo(json.dumps(data, indent=2))
        elif use_format == 'table':
            if 'data' in data and data['data']:
                table = Table(title="API Keys")
                table.add_column("ID", style="cyan")
                table.add_column("Name", style="white")
                table.add_column("Type", style="green")
                table.add_column("Generated On", style="yellow")
                
                for key in data['data']:
                    table.add_row(
                        key.get('id', ''),
                        key.get('name', ''),
                        key.get('type', ''),
                        format_timestamp(key.get('generated_on', ''))
                    )
                console.print(table)
            else:
                console.print("No API keys found", style="yellow")
        else:  # simple
            if 'data' in data and data['data']:
                for key in data['data']:
                    click.echo(f"{key.get('name', '')} {key.get('id', '')}")
            else:
                click.echo("No API keys found")
                
    except (APIError, ConfigurationError) as e:
        click.echo(f"❌ {e}", err=True)

@keys_group.command(name='add')
@click.argument('name')
@click.option('--type', 'key_type', default='USER', type=click.Choice(['USER', 'ORGANIZATION']), help='API key type')
@click.option('--organization-id', help='Organization ID (for ORGANIZATION type keys)')
@click.option('--output', type=click.Choice(['simple', 'table', 'json']), help='Output format')
@click.pass_context
def add_key(ctx, name, key_type, organization_id, output):
    """Generate a new API key"""
    client, config_manager = get_client_and_config(ctx)
    if output:
        use_format = output
    elif not sys.stdout.isatty():
        use_format = 'simple'
    else:
        use_format = config_manager.get('default_output', 'simple')
    
    try:
        if key_type == 'ORGANIZATION' and not organization_id:
            click.echo("❌ Organization ID is required for ORGANIZATION type keys", err=True)
            ctx.exit(1)
        
        data = client.create_api_key(name, key_type, organization_id)
        
        if use_format == 'json':
            click.echo(json.dumps(data, indent=2))
        elif use_format == 'table':
            table = Table(title="New API Key Created")
            table.add_column("Field", style="cyan")
            table.add_column("Value", style="white")
            
            key_data = data.get('data', data)
            table.add_row("ID", key_data.get('id', ''))
            table.add_row("Name", key_data.get('name', ''))
            table.add_row("Type", key_data.get('type', ''))
            table.add_row("Key", key_data.get('key', 'Not provided'))
            table.add_row("Generated On", format_timestamp(key_data.get('generated_on', '')))
            
            console.print(table)
            if 'key' in key_data:
                console.print("[bold red]⚠️  Save this key securely - it cannot be retrieved again![/bold red]")
        else:  # simple
            key_data = data.get('data', data)
            click.echo(f"✅ API key '{name}' created successfully")
            click.echo(f"ID: {key_data.get('id', '')}")
            if 'key' in key_data:
                click.echo(f"Key: {key_data['key']}")
                click.echo("⚠️  Save this key securely - it cannot be retrieved again!")
                
    except (APIError, ConfigurationError) as e:
        click.echo(f"❌ {e}", err=True)

@keys_group.command(name='delete')
@click.argument('key_id')
@click.option('--confirm', is_flag=True, help='Skip confirmation prompt')
@click.pass_context
def delete_key(ctx, key_id, confirm):
    """Delete an API key"""
    client, config_manager = get_client_and_config(ctx)
    
    try:
        if not confirm:
            if not click.confirm(f"Are you sure you want to delete API key {key_id}?"):
                click.echo("Operation cancelled")
                return
        
        result = client.delete_api_key(key_id)
        click.echo(f"✅ {result['message']}")
                
    except (APIError, ConfigurationError) as e:
        click.echo(f"❌ {e}", err=True)

# Users subcommands
@account_group.group(name='users')
def users_group():
    """Manage users"""
    pass

@users_group.command(name='list')
@click.option('--output', type=click.Choice(['simple', 'table', 'json']), help='Output format')
@click.option('--no-cache', is_flag=True, help='Disable caching for this query')
@click.pass_context
def list_users(ctx, output, no_cache):
    """List all users"""
    client, config_manager = get_client_and_config(ctx)
    if output:
        use_format = output
    elif not sys.stdout.isatty():
        use_format = 'simple'
    else:
        use_format = config_manager.get('default_output', 'simple')
    
    try:
        cache_key = "users"
        cached_result = None
        if client.cache_manager and not no_cache:
            cached_result = client.cache_manager.get('account_api', cache_key)
            if cached_result and use_format != 'json':
                console.print("📋 Using cached result", style="dim")
        
        if not cached_result:
            data = client.list_users()
            if client.cache_manager and not no_cache:
                client.cache_manager.set('account_api', cache_key, data)
        else:
            data = cached_result
        
        if use_format == 'json':
            click.echo(json.dumps(data, indent=2))
        elif use_format == 'table':
            # Handle both data wrapper and direct array response
            users_list = data.get('data', data) if isinstance(data, dict) and 'data' in data else data
            if users_list:
                table = Table(title="Users")
                table.add_column("ID", style="cyan")
                table.add_column("Email", style="white")
                table.add_column("Name", style="yellow")
                table.add_column("Status", style="green")
                
                for user in users_list:
                    table.add_row(
                        user.get('id', ''),
                        user.get('email', ''),
                        f"{user.get('first_name', '')} {user.get('last_name', '')}".strip(),
                        user.get('status', '')
                    )
                console.print(table)
            else:
                console.print("No users found", style="yellow")
        else:  # simple
            users_list = data.get('data', data) if isinstance(data, dict) and 'data' in data else data
            if users_list:
                for user in users_list:
                    click.echo(f"{user.get('email', '')} {user.get('id', '')}")
            else:
                click.echo("No users found")
                
    except (APIError, ConfigurationError) as e:
        click.echo(f"❌ {e}", err=True)

@users_group.command(name='get')
@click.argument('user_id')
@click.option('--output', type=click.Choice(['simple', 'table', 'json']), help='Output format')
@click.option('--no-cache', is_flag=True, help='Disable caching for this query')
@click.pass_context
def get_user(ctx, user_id, output, no_cache):
    """Get specific user details"""
    client, config_manager = get_client_and_config(ctx)
    if output:
        use_format = output
    elif not sys.stdout.isatty():
        use_format = 'simple'
    else:
        use_format = config_manager.get('default_output', 'simple')
    
    try:
        cache_key = f"user_{user_id}"
        cached_result = None
        if client.cache_manager and not no_cache:
            cached_result = client.cache_manager.get('account_api', cache_key)
            if cached_result and use_format != 'json':
                console.print("📋 Using cached result", style="dim")
        
        if not cached_result:
            data = client.get_user(user_id)
            if client.cache_manager and not no_cache:
                client.cache_manager.set('account_api', cache_key, data)
        else:
            data = cached_result
        
        if use_format == 'json':
            click.echo(json.dumps(data, indent=2))
        elif use_format == 'table':
            user_data = data.get('data', data) if 'data' in data else data
            
            table = Table(title=f"User Details: {user_id}")
            table.add_column("Field", style="cyan")
            table.add_column("Value", style="white")
            
            table.add_row("ID", user_data.get('id', ''))
            table.add_row("Email", user_data.get('email', ''))
            table.add_row("First Name", user_data.get('first_name', ''))
            table.add_row("Last Name", user_data.get('last_name', ''))
            table.add_row("Status", user_data.get('status', ''))
            table.add_row("Created", format_timestamp(user_data.get('created_on', '')))
            table.add_row("Last Login", format_timestamp(user_data.get('last_login', '')))
            
            console.print(table)
        else:  # simple
            user_data = data.get('data', data) if 'data' in data else data
            click.echo(f"Email: {user_data.get('email', '')}")
            click.echo(f"Name: {user_data.get('first_name', '')} {user_data.get('last_name', '')}".strip())
            click.echo(f"Status: {user_data.get('status', '')}")
                
    except (APIError, ConfigurationError) as e:
        click.echo(f"❌ {e}", err=True)

# Organizations subcommands
@account_group.group(name='orgs')
def orgs_group():
    """Manage organizations"""
    pass

@orgs_group.command(name='list')
@click.option('--output', type=click.Choice(['simple', 'table', 'json']), help='Output format')
@click.option('--no-cache', is_flag=True, help='Disable caching for this query')
@click.pass_context
def list_orgs(ctx, output, no_cache):
    """List all organizations"""
    client, config_manager = get_client_and_config(ctx)
    if output:
        use_format = output
    elif not sys.stdout.isatty():
        use_format = 'simple'
    else:
        use_format = config_manager.get('default_output', 'simple')
    
    try:
        cache_key = "organizations"
        cached_result = None
        if client.cache_manager and not no_cache:
            cached_result = client.cache_manager.get('account_api', cache_key)
            if cached_result and use_format != 'json':
                console.print("📋 Using cached result", style="dim")
        
        if not cached_result:
            data = client.list_organizations()
            if client.cache_manager and not no_cache:
                client.cache_manager.set('account_api', cache_key, data)
        else:
            data = cached_result
        
        if use_format == 'json':
            click.echo(json.dumps(data, indent=2))
        elif use_format == 'table':
            # Handle both data wrapper and direct array response
            orgs_list = data.get('data', data) if isinstance(data, dict) and 'data' in data else data
            if orgs_list:
                table = Table(title="Organizations")
                table.add_column("ID", style="cyan")
                table.add_column("Name", style="white")
                table.add_column("Region", style="yellow")
                
                for org in orgs_list:
                    table.add_row(
                        org.get('id', ''),
                        org.get('name', ''),
                        org.get('type', org.get('region', ''))  # Fall back to region if no type
                    )
                console.print(table)
            else:
                console.print("No organizations found", style="yellow")
        else:  # simple
            orgs_list = data.get('data', data) if isinstance(data, dict) and 'data' in data else data
            if orgs_list:
                for org in orgs_list:
                    click.echo(f"{org.get('name', '')} {org.get('id', '')}")
            else:
                click.echo("No organizations found")
                
    except (APIError, ConfigurationError) as e:
        click.echo(f"❌ {e}", err=True)

# Products subcommands
@account_group.group(name='products')
def products_group():
    """Manage products"""
    pass

@products_group.command(name='list')
@click.option('--output', type=click.Choice(['simple', 'table', 'json']), help='Output format')
@click.option('--no-cache', is_flag=True, help='Disable caching for this query')
@click.pass_context
def list_products(ctx, output, no_cache):
    """List all products"""
    client, config_manager = get_client_and_config(ctx)
    if output:
        use_format = output
    elif not sys.stdout.isatty():
        use_format = 'simple'
    else:
        use_format = config_manager.get('default_output', 'simple')
    
    try:
        cache_key = "products"
        cached_result = None
        if client.cache_manager and not no_cache:
            cached_result = client.cache_manager.get('account_api', cache_key)
            if cached_result and use_format != 'json':
                console.print("📋 Using cached result", style="dim")
        
        if not cached_result:
            data = client.list_products()
            if client.cache_manager and not no_cache:
                client.cache_manager.set('account_api', cache_key, data)
        else:
            data = cached_result
        
        if use_format == 'json':
            click.echo(json.dumps(data, indent=2))
        elif use_format == 'table':
            # Handle both data wrapper and direct array response
            products_list = data.get('data', data) if isinstance(data, dict) and 'data' in data else data
            if products_list:
                table = Table(title="Products")
                table.add_column("Token", style="cyan")
                table.add_column("Code", style="white")
                table.add_column("Organization", style="green")
                
                for product in products_list:
                    table.add_row(
                        product.get('product_token', ''),
                        product.get('product_code', ''),
                        product.get('organization_name', '')
                    )
                console.print(table)
            else:
                console.print("No products found", style="yellow")
        else:  # simple
            products_list = data.get('data', data) if isinstance(data, dict) and 'data' in data else data
            if products_list:
                for product in products_list:
                    click.echo(f"{product.get('product_code', '')} {product.get('product_token', '')}")
            else:
                click.echo("No products found")
                
    except (APIError, ConfigurationError) as e:
        click.echo(f"❌ {e}", err=True)

@products_group.command(name='get')
@click.argument('product_token')
@click.option('--output', type=click.Choice(['simple', 'table', 'json']), help='Output format')
@click.option('--no-cache', is_flag=True, help='Disable caching for this query')
@click.pass_context
def get_product(ctx, product_token, output, no_cache):
    """Get product details"""
    client, config_manager = get_client_and_config(ctx)
    if output:
        use_format = output
    elif not sys.stdout.isatty():
        use_format = 'simple'
    else:
        use_format = config_manager.get('default_output', 'simple')
    
    try:
        cache_key = f"product_{product_token}"
        cached_result = None
        if client.cache_manager and not no_cache:
            cached_result = client.cache_manager.get('account_api', cache_key)
            if cached_result and use_format != 'json':
                console.print("📋 Using cached result", style="dim")
        
        if not cached_result:
            data = client.get_product(product_token)
            if client.cache_manager and not no_cache:
                client.cache_manager.set('account_api', cache_key, data)
        else:
            data = cached_result
        
        if use_format == 'json':
            click.echo(json.dumps(data, indent=2))
        elif use_format == 'table':
            product_data = data.get('data', data) if 'data' in data else data
            
            table = Table(title=f"Product Details: {product_token}")
            table.add_column("Field", style="cyan")
            table.add_column("Value", style="white")
            
            table.add_row("Token", product_data.get('product_token', ''))
            table.add_row("Code", product_data.get('product_code', ''))
            table.add_row("Organization", product_data.get('organization_name', ''))
            table.add_row("Organization ID", product_data.get('organization_id', ''))
            
            console.print(table)
        else:  # simple
            product_data = data.get('data', data) if 'data' in data else data
            click.echo(f"Code: {product_data.get('product_code', '')}")
            click.echo(f"Token: {product_data.get('product_token', '')}")
            click.echo(f"Organization: {product_data.get('organization_name', '')}")
                
    except (APIError, ConfigurationError) as e:
        click.echo(f"❌ {e}", err=True)

@products_group.command(name='list-users')
@click.argument('product_token')
@click.option('--output', type=click.Choice(['simple', 'table', 'json']), help='Output format')
@click.option('--no-cache', is_flag=True, help='Disable caching for this query')
@click.pass_context
def list_product_users(ctx, product_token, output, no_cache):
    """List users with access to a product"""
    client, config_manager = get_client_and_config(ctx)
    if output:
        use_format = output
    elif not sys.stdout.isatty():
        use_format = 'simple'
    else:
        use_format = config_manager.get('default_output', 'simple')
    
    try:
        cache_key = f"product_users_{product_token}"
        cached_result = None
        if client.cache_manager and not no_cache:
            cached_result = client.cache_manager.get('account_api', cache_key)
            if cached_result and use_format != 'json':
                console.print("📋 Using cached result", style="dim")
        
        if not cached_result:
            data = client.list_product_users(product_token)
            if client.cache_manager and not no_cache:
                client.cache_manager.set('account_api', cache_key, data)
        else:
            data = cached_result
        
        if use_format == 'json':
            click.echo(json.dumps(data, indent=2))
        elif use_format == 'table':
            # Handle both data wrapper and direct array response
            users_list = data.get('data', data) if isinstance(data, dict) and 'data' in data else data
            if users_list:
                table = Table(title=f"Users with access to {product_token}")
                table.add_column("ID", style="cyan")
                table.add_column("Email", style="white")
                table.add_column("Name", style="yellow")
                table.add_column("Status", style="green")
                
                for user in users_list:
                    table.add_row(
                        user.get('id', ''),
                        user.get('email', ''),
                        f"{user.get('first_name', '')} {user.get('last_name', '')}".strip(),
                        user.get('status', '')
                    )
                console.print(table)
            else:
                console.print(f"No users found for product {product_token}", style="yellow")
        else:  # simple
            users_list = data.get('data', data) if isinstance(data, dict) and 'data' in data else data
            if users_list:
                for user in users_list:
                    click.echo(f"{user.get('email', '')} {user.get('id', '')}")
            else:
                click.echo(f"No users found for product {product_token}")
                
    except (APIError, ConfigurationError) as e:
        click.echo(f"❌ {e}", err=True)

# Roles subcommands
@account_group.group(name='roles')
def roles_group():
    """Manage roles"""
    pass

@roles_group.command(name='list')
@click.option('--output', type=click.Choice(['simple', 'table', 'json']), help='Output format')
@click.option('--no-cache', is_flag=True, help='Disable caching for this query')
@click.pass_context
def list_roles(ctx, output, no_cache):
    """List all roles"""
    client, config_manager = get_client_and_config(ctx)
    if output:
        use_format = output
    elif not sys.stdout.isatty():
        use_format = 'simple'
    else:
        use_format = config_manager.get('default_output', 'simple')
    
    try:
        cache_key = "roles"
        cached_result = None
        if client.cache_manager and not no_cache:
            cached_result = client.cache_manager.get('account_api', cache_key)
            if cached_result and use_format != 'json':
                console.print("📋 Using cached result", style="dim")
        
        if not cached_result:
            data = client.list_roles()
            if client.cache_manager and not no_cache:
                client.cache_manager.set('account_api', cache_key, data)
        else:
            data = cached_result
        
        if use_format == 'json':
            click.echo(json.dumps(data, indent=2))
        elif use_format == 'table':
            # Handle both data wrapper and direct array response
            roles_list = data.get('data', data) if isinstance(data, dict) and 'data' in data else data
            if roles_list:
                table = Table(title="Roles")
                table.add_column("ID", style="cyan")
                table.add_column("Name", style="white")
                table.add_column("Description", style="yellow")
                table.add_column("Products", style="green")
                
                for role in roles_list:
                    # Get product codes from supported_products
                    products = ', '.join([p.get('product_code', '') for p in role.get('supported_products', [])])
                    table.add_row(
                        role.get('id', ''),
                        role.get('name', ''),
                        role.get('description', '')[:50] + ('...' if len(role.get('description', '')) > 50 else ''),
                        products[:30] + ('...' if len(products) > 30 else '')
                    )
                console.print(table)
            else:
                console.print("No roles found", style="yellow")
        else:  # simple
            roles_list = data.get('data', data) if isinstance(data, dict) and 'data' in data else data
            if roles_list:
                for role in roles_list:
                    click.echo(f"{role.get('name', '')} {role.get('id', '')}")
            else:
                click.echo("No roles found")
                
    except (APIError, ConfigurationError) as e:
        click.echo(f"❌ {e}", err=True)

@roles_group.command(name='get')
@click.argument('role_id')
@click.option('--output', type=click.Choice(['simple', 'table', 'json']), help='Output format')
@click.option('--no-cache', is_flag=True, help='Disable caching for this query')
@click.pass_context
def get_role(ctx, role_id, output, no_cache):
    """Get role details"""
    client, config_manager = get_client_and_config(ctx)
    if output:
        use_format = output
    elif not sys.stdout.isatty():
        use_format = 'simple'
    else:
        use_format = config_manager.get('default_output', 'simple')
    
    try:
        cache_key = f"role_{role_id}"
        cached_result = None
        if client.cache_manager and not no_cache:
            cached_result = client.cache_manager.get('account_api', cache_key)
            if cached_result and use_format != 'json':
                console.print("📋 Using cached result", style="dim")
        
        if not cached_result:
            data = client.get_role(role_id)
            if client.cache_manager and not no_cache:
                client.cache_manager.set('account_api', cache_key, data)
        else:
            data = cached_result
        
        if use_format == 'json':
            click.echo(json.dumps(data, indent=2))
        elif use_format == 'table':
            role_data = data.get('data', data) if 'data' in data else data
            
            table = Table(title=f"Role Details: {role_id}")
            table.add_column("Field", style="cyan")
            table.add_column("Value", style="white")
            
            table.add_row("ID", role_data.get('id', ''))
            table.add_row("Name", role_data.get('name', ''))
            table.add_row("Type", role_data.get('type', ''))
            table.add_row("Product", role_data.get('product', ''))
            table.add_row("Description", role_data.get('description', ''))
            
            console.print(table)
        else:  # simple
            role_data = data.get('data', data) if 'data' in data else data
            click.echo(f"Name: {role_data.get('name', '')}")
            click.echo(f"Type: {role_data.get('type', '')}")
            click.echo(f"Product: {role_data.get('product', '')}")
                
    except (APIError, ConfigurationError) as e:
        click.echo(f"❌ {e}", err=True)

@roles_group.command(name='delete')
@click.argument('role_id')
@click.option('--confirm', is_flag=True, help='Skip confirmation prompt')
@click.pass_context
def delete_role(ctx, role_id, confirm):
    """Delete a role"""
    client, config_manager = get_client_and_config(ctx)
    
    try:
        if not confirm:
            if not click.confirm(f"Are you sure you want to delete role {role_id}?"):
                click.echo("Operation cancelled")
                return
        
        result = client.delete_role(role_id)
        click.echo(f"✅ {result['message']}")
                
    except (APIError, ConfigurationError) as e:
        click.echo(f"❌ {e}", err=True)

# Resource Groups subcommands
@account_group.group(name='resource-groups')
def resource_groups_group():
    """Manage resource groups"""
    pass

@resource_groups_group.command(name='list')
@click.option('--output', type=click.Choice(['simple', 'table', 'json']), help='Output format')
@click.option('--no-cache', is_flag=True, help='Disable caching for this query')
@click.pass_context
def list_resource_groups(ctx, output, no_cache):
    """List all resource groups"""
    client, config_manager = get_client_and_config(ctx)
    if output:
        use_format = output
    elif not sys.stdout.isatty():
        use_format = 'simple'
    else:
        use_format = config_manager.get('default_output', 'simple')
    
    try:
        cache_key = "resource_groups"
        cached_result = None
        if client.cache_manager and not no_cache:
            cached_result = client.cache_manager.get('account_api', cache_key)
            if cached_result and use_format != 'json':
                console.print("📋 Using cached result", style="dim")
        
        if not cached_result:
            data = client.list_resource_groups()
            if client.cache_manager and not no_cache:
                client.cache_manager.set('account_api', cache_key, data)
        else:
            data = cached_result
        
        if use_format == 'json':
            click.echo(json.dumps(data, indent=2))
        elif use_format == 'table':
            # Combine both granular and non-granular control groups
            all_groups = []
            if 'granular_control_resource_groups' in data:
                for group in data['granular_control_resource_groups']:
                    group['granular_control'] = True
                    all_groups.append(group)
            if 'non_granular_control_resource_groups' in data:
                for group in data['non_granular_control_resource_groups']:
                    group['granular_control'] = False
                    all_groups.append(group)
            
            if all_groups:
                table = Table(title="Resource Groups")
                table.add_column("ID", style="cyan")
                table.add_column("Name", style="white")
                table.add_column("Granular Control", style="green")
                
                for group in all_groups:
                    table.add_row(
                        group.get('id', ''),
                        group.get('name', ''),
                        'Yes' if group.get('granular_control') else 'No'
                    )
                console.print(table)
            else:
                console.print("No resource groups found", style="yellow")
        else:  # simple
            # Combine both types for simple output too
            all_groups = []
            if 'granular_control_resource_groups' in data:
                all_groups.extend(data['granular_control_resource_groups'])
            if 'non_granular_control_resource_groups' in data:
                all_groups.extend(data['non_granular_control_resource_groups'])
            
            if all_groups:
                for group in all_groups:
                    click.echo(f"{group.get('name', '')} {group.get('id', '')}")
            else:
                click.echo("No resource groups found")
                
    except (APIError, ConfigurationError) as e:
        click.echo(f"❌ {e}", err=True)

# Features subcommands
@account_group.group(name='features')
def features_group():
    """Manage features"""
    pass

@features_group.command(name='list')
@click.option('--output', type=click.Choice(['simple', 'table', 'json']), help='Output format')
@click.option('--no-cache', is_flag=True, help='Disable caching for this query')
@click.pass_context
def list_features(ctx, output, no_cache):
    """List all features and permissions"""
    client, config_manager = get_client_and_config(ctx)
    if output:
        use_format = output
    elif not sys.stdout.isatty():
        use_format = 'simple'
    else:
        use_format = config_manager.get('default_output', 'simple')
    
    try:
        cache_key = "features"
        cached_result = None
        if client.cache_manager and not no_cache:
            cached_result = client.cache_manager.get('account_api', cache_key)
            if cached_result and use_format != 'json':
                console.print("📋 Using cached result", style="dim")
        
        if not cached_result:
            data = client.list_features()
            if client.cache_manager and not no_cache:
                client.cache_manager.set('account_api', cache_key, data)
        else:
            data = cached_result
        
        if use_format == 'json':
            click.echo(json.dumps(data, indent=2))
        elif use_format == 'table':
            # Handle both data wrapper and direct array response
            features_list = data.get('data', data) if isinstance(data, dict) and 'data' in data else data
            if features_list:
                table = Table(title="Features")
                table.add_column("ID", style="cyan")
                table.add_column("Name", style="white")
                table.add_column("Description", style="yellow")
                table.add_column("Permissions", style="green")
                
                for feature in features_list:
                    permissions = ', '.join([p.get('name', '') for p in feature.get('permissions', [])])
                    table.add_row(
                        feature.get('id', '')[:20] + ('...' if len(feature.get('id', '')) > 20 else ''),
                        feature.get('name', ''),
                        feature.get('description', '')[:40] + ('...' if len(feature.get('description', '')) > 40 else ''),
                        permissions[:30] + ('...' if len(permissions) > 30 else '')
                    )
                console.print(table)
            else:
                console.print("No features found", style="yellow")
        else:  # simple
            features_list = data.get('data', data) if isinstance(data, dict) and 'data' in data else data
            if features_list:
                for feature in features_list:
                    click.echo(f"{feature.get('name', '')} {feature.get('id', '')}")
            else:
                click.echo("No features found")
                
    except (APIError, ConfigurationError) as e:
        click.echo(f"❌ {e}", err=True)

