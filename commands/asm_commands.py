import json
import sys
import subprocess
import click
from rich.console import Console
from rich.table import Table
from rich.progress import Progress, SpinnerColumn, TextColumn, TimeElapsedColumn
from api.client import Rapid7Client
from utils.config import ConfigManager
from utils.cache import CacheManager
from utils.credentials import CredentialManager
from utils.exceptions import *

console = Console()

def _parse_columns_arg(columns_str):
    """Parse --columns which may be JSON or csv like 'm.name,m.asset_class'.
    Returns a tuple (columns_list, normalized_str_for_cache).
    """
    if not columns_str or columns_str.strip() in ('[]', 'none', 'None', 'auto'):
        return [], '[]'
    text = columns_str.strip()
    # If it looks like JSON, parse and normalize
    if text.startswith('['):
        try:
            cols = json.loads(text)
            # Normalize ordering and serialize
            norm = json.dumps(cols, separators=(',', ':'), sort_keys=True)
            return cols, norm
        except json.JSONDecodeError:
            raise QueryError("Invalid JSON for --columns. Use JSON or csv 'alias.prop,alias.prop'")
    # Otherwise, parse csv
    parts = [p.strip() for p in text.split(',') if p.strip()]
    cols = []
    for p in parts:
        if '.' in p:
            alias, prop = p.split('.', 1)
            cols.append({"alias": alias.strip(), "property_name": prop.strip()})
        else:
            # Only property provided; leave alias empty
            cols.append({"alias": "", "property_name": p})
    norm = json.dumps(cols, separators=(',', ':'), sort_keys=True)
    return cols, norm

def should_use_json_output(output_format, config_default):
    """Determine if we should use JSON output based on pipe detection and user preference"""
    if output_format:
        return output_format == 'json'
    if not sys.stdout.isatty():
        return True
    return config_default == 'json'

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

@click.group(name='asm')
def asm_group():
    """surface command cypher queries, apps/sdk"""
    pass

@asm_group.command(name='profile')
@click.option('--output', type=click.Choice(['table', 'json']), help='Output format')
@click.pass_context
def asm_profile(ctx, output):
    """Get Surface Command user profile information"""
    client, config_manager = get_client_and_config(ctx)
    use_json = should_use_json_output(output, config_manager.get('default_output'))
    
    try:
        base_url = f"https://{client.region}.api.insight.rapid7.com/surface/auth-api/profile"
        response = client.make_request("GET", base_url)
        
        if response.status_code != 200:
            raise APIError(f"Profile request failed: {response.status_code} - {response.text}")
        
        data = response.json()
        
        if use_json:
            click.echo(json.dumps(data, indent=2))
        else:
            if data:
                table = Table(title="Surface Command Profile")
                table.add_column("Field", style="cyan")
                table.add_column("Value", style="white")
                
                # Extract data from id_token if present, otherwise use top level
                id_token = data.get('id_token', {})
                
                # Display profile information in a structured way
                table.add_row("User ID", str(id_token.get('sub', '')))
                table.add_row("Email", str(id_token.get('email', '')))
                table.add_row("Name", str(id_token.get('name', '')))
                table.add_row("Given Name", str(id_token.get('given_name', '')))
                table.add_row("Family Name", str(id_token.get('family_name', '')))
                table.add_row("Username", str(id_token.get('preferred_username', '')))
                table.add_row("Customer ID", str(id_token.get('customer_id', '')))
                table.add_row("Organization ID", str(id_token.get('org_id', '')))
                
                # Handle permission roles
                if 'permission_roles' in id_token and id_token['permission_roles']:
                    table.add_row("Permission Roles", ', '.join(id_token['permission_roles']))
                
                # Handle features
                if 'features' in id_token and id_token['features']:
                    table.add_row("Features", ', '.join(id_token['features']))
                
                # Handle capabilities
                if 'capabilities' in id_token and id_token['capabilities']:
                    table.add_row("Capabilities", ', '.join(id_token['capabilities']))
                
                # Handle license information
                if 'license' in id_token:
                    license_info = id_token['license']
                    table.add_row("License ID", str(license_info.get('license_id', '')))
                    table.add_row("License Name", str(license_info.get('license_name', '')))
                
                table.add_row("License Type", str(id_token.get('license_type', '')))
                table.add_row("License Status", str(id_token.get('license_status', '')))
                
                console.print(table)
            else:
                console.print("No profile data found", style="yellow")
                
    except (APIError, ConfigurationError) as e:
        click.echo(f"❌ {e}", err=True)

@asm_group.command(name='apps')
@click.option('--output', type=click.Choice(['table', 'json']), help='Output format')
@click.option('--no-cache', is_flag=True, help='Disable caching for this request')
@click.option('--all-types', is_flag=True, help='Show all types instead of truncating to first 3')
@click.option('--exclude-apps', help='Comma-separated list of app IDs to exclude from output')
@click.pass_context
def asm_apps(ctx, output, no_cache, all_types, exclude_apps):
    """List Surface Command apps"""
    client, config_manager = get_client_and_config(ctx)
    use_json = should_use_json_output(output, config_manager.get('default_output'))
    
    try:
        # Check cache first
        cache_key = "surface_apps_list"
        cached_result = None
        if client.cache_manager and not no_cache:
            cached_result = client.cache_manager.get('surface_apps', cache_key)
            if cached_result:
                if not use_json:
                    console.print("📋 Using cached result", style="dim")
        
        if not cached_result:
            data = client.list_surface_apps()
            # Cache the result
            if client.cache_manager and not no_cache:
                client.cache_manager.set('surface_apps', cache_key, data)
        else:
            data = cached_result
        
        # Apply exclusion filter if provided
        if exclude_apps and data and isinstance(data, dict):
            exclude_list = [app_id.strip() for app_id in exclude_apps.split(',') if app_id.strip()]
            if exclude_list:
                original_count = len(data)
                filtered_data = {app_id: app_data for app_id, app_data in data.items() if app_id not in exclude_list}
                data = filtered_data
                excluded_count = original_count - len(data)
                if not use_json and excluded_count > 0:
                    console.print(f"[dim]📝 Excluded {excluded_count} app(s) from output[/dim]")
        
        if use_json:
            click.echo(json.dumps(data, indent=2))
        else:
            # Display in table format - API returns apps as a dictionary with app IDs as keys
            if not data or not isinstance(data, dict):
                console.print("No Surface Command apps found", style="yellow")
                return
            
            table = Table(title="Surface Command Apps")
            table.add_column("App ID", style="cyan", width=40)
            table.add_column("Name", style="white", width=30)
            table.add_column("Version", style="yellow", width=8)
            table.add_column("Types", style="blue", width=40)
            table.add_column("Created", style="dim", width=10)
            
            # Sort apps by name for consistent display
            sorted_apps = sorted(data.items(), key=lambda x: x[1].get('name', x[0]))
            
            for app_id, app_data in sorted_apps:
                # Extract fields safely
                name = app_data.get('name', 'Unknown')
                description = app_data.get('description', '')
                version = app_data.get('version', 'Unknown')
                publisher = app_data.get('publisher', 'Unknown')
                categories = app_data.get('categories', [])
                types = app_data.get('types', [])
                
                # Format creation date from stored_object_metadata
                created_display = ''
                metadata = app_data.get('stored_object_metadata', {})
                created_at = metadata.get('created', '')
                if created_at:
                    try:
                        from datetime import datetime
                        if 'T' in created_at:
                            dt = datetime.fromisoformat(created_at.replace('Z', '+00:00'))
                            created_display = dt.strftime('%m-%d')
                        else:
                            created_display = created_at[:5]  # Just MM-DD
                    except Exception:
                        created_display = created_at[:5] if created_at else ''
                
                # Format types - show actual type names with line breaks for readability
                if types:
                    if all_types or len(types) <= 3:
                        # Show all types on separate lines
                        types_display = '\n'.join(types)
                    else:
                        # Show first 3 types + count of remaining
                        types_display = '\n'.join(types[:3])
                        remaining = len(types) - 3
                        types_display += f'\n+ {remaining} more'
                else:
                    types_display = "None"
                
                # Use full app ID without truncation
                display_id = app_id
                
                table.add_row(
                    display_id,
                    name,
                    version,
                    types_display,
                    created_display
                )
            
            console.print(table)
            
            # Show summary
            total_apps = len(data)
            console.print(f"\n[dim]Total apps: {total_apps}[/dim]")
                
    except (APIError, ConfigurationError) as e:
        click.echo(f"❌ {e}", err=True)

@asm_group.group(name='cypher')
def cypher_group():
    """ASM Cypher query commands"""
    pass

@cypher_group.command(name='query')
@click.argument('query')
@click.option('--columns', default='[]', help='JSON array of columns (e.g., [{"alias":"m","property_name":"name"}])')
@click.option('--output', type=click.Choice(['table', 'json']), help='Output format')
@click.option('--limit', type=int, default=100, show_default=True, help='Max rows to request and display')
@click.option('--start', type=int, default=0, show_default=True, help='Pagination offset - position in result set to start')
@click.option('--depth', type=int, default=0, show_default=True, help='Graph traversal depth for nested relationships')
@click.option('--order/--no-order', default=True, show_default=True, help='Enable/disable result ordering')
@click.option('--use-primary/--no-use-primary', default=False, show_default=True, help='Use primary properties for selection')
@click.option('--no-cache', is_flag=True, help='Disable caching for this query')
@click.pass_context
def cypher_query(ctx, query, columns, output, limit, start, depth, order, use_primary, no_cache):
    """Execute ASM Cypher queries"""
    client, config_manager = get_client_and_config(ctx)
    use_json = should_use_json_output(output, config_manager.get('default_output'))
    try:
        # Parse columns flexibly
        columns_parsed, columns_norm = _parse_columns_arg(columns)
        
        # Build query parameters to match UI behavior
        query_params = {
            'start': start,
            'length': limit,  # Use limit for API request size
            'depth': depth,
            'order': str(order).lower(),
            'use_primary': str(use_primary).lower(),
            'format': 'json'
        }
        
        # Create cache key including all parameters that affect results
        cache_key = f"{query}_{columns_norm}_{start}_{limit}_{depth}_{order}_{use_primary}"
        
        cached_result = None
        if client.cache_manager and not no_cache:
            cached_result = client.cache_manager.get('cypher_query', cache_key)
            if cached_result and not use_json:
                console.print("📋 Using cached result", style="dim")
        if not cached_result:
            base_url = client.get_base_url('asm')
            # Build URL with all query parameters
            param_string = '&'.join([f"{k}={v}" for k, v in query_params.items()])
            url = f"{base_url}?{param_string}"
            body = {"columns": columns_parsed, "cypher": query}
            if use_json:
                response = client.make_request("POST", url, data=body)
            else:
                with Progress(
                    SpinnerColumn(),
                    TextColumn("Executing Cypher query..."),
                    TimeElapsedColumn(),
                ) as progress:
                    _ = progress.add_task("Querying...", total=None)
                    response = client.make_request("POST", url, data=body)
            if response.status_code != 200:
                raise APIError(f"Query failed: {response.status_code} - {response.text}")
            data = response.json()
            if client.cache_manager and not no_cache:
                client.cache_manager.set('cypher_query', cache_key, data)
        else:
            data = cached_result
        if use_json:
            click.echo(json.dumps(data, indent=2))
        else:
            if 'items' in data and data['items']:
                columns_config = columns_parsed or []
                # Determine headers safely
                # If columns are provided, use them; otherwise infer from first row length
                first_row = data['items'][0].get('data', [])
                inferred_len = len(first_row) if isinstance(first_row, list) else 1

                if columns_config:
                    col_headers = []
                    for i, col in enumerate(columns_config):
                        alias = col.get('alias') or ''
                        prop = col.get('property_name') or ''
                        header = f"{alias}.{prop}" if alias and prop else (prop or alias or f"Value {i+1}")
                        col_headers.append(header)
                    # If server returns more cells than provided columns, pad headers
                    if inferred_len > len(col_headers):
                        extra = [f"Value {i+1}" for i in range(len(col_headers), inferred_len)]
                        col_headers.extend(extra)
                else:
                    # No columns specified; create generic headers based on inferred length
                    col_headers = [f"Value {i+1}" for i in range(inferred_len)]

                table = Table(title="ASM Query Results")
                for header in col_headers:
                    table.add_column(header, style="cyan")

                # Display all items (API already limited by --limit parameter)
                for item in data['items']:
                    row_data = item.get('data', [])
                    if not isinstance(row_data, list):
                        row_data = [row_data]

                    pretty_cells = []
                    for cell in row_data[: len(col_headers)]:
                        if cell is None:
                            pretty_cells.append('')
                        elif isinstance(cell, list):
                            pretty_cells.append(', '.join([str(x) for x in cell]))
                        else:
                            pretty_cells.append(str(cell))
                    # Pad if fewer cells than headers
                    if len(pretty_cells) < len(col_headers):
                        pretty_cells.extend([''] * (len(col_headers) - len(pretty_cells)))
                    table.add_row(*pretty_cells)

                console.print(table)
                # Show info about pagination if using start > 0
                if start > 0:
                    console.print(f"[dim]Showing results starting from position {start} (use --start to paginate)[/dim]")
            else:
                console.print("No results found", style="yellow")
    except (APIError, QueryError) as e:
        click.echo(f"❌ {e}", err=True)

@cypher_group.command(name='docs')
def cypher_docs():
    """Show Cypher DSL reference guide"""
    try:
        import os
        from pathlib import Path
        
        # Get the path to the Cypher reference file
        current_dir = Path(__file__).parent.parent
        cypher_file = current_dir / 'docs/cypher-dsl.md'
        
        if not cypher_file.exists():
            click.echo("❌ Cypher reference file not found: cypher-dsl.md", err=True)
            return
            
        # Read and display the markdown file
        with open(cypher_file, 'r', encoding='utf-8') as f:
            content = f.read()
            
        # Output as plain text to avoid centered headers
        click.echo(content)
            
    except Exception as e:
        click.echo(f"❌ Error displaying Cypher help: {e}", err=True)


@cypher_group.command(name='examples')
@click.option('--output', type=click.Choice(['table', 'json', 'plain', 'cmd']), default='plain', help='How to display the examples')
@click.option('--test', is_flag=True, help='Execute each example and report results (requires valid API key & region)')
@click.pass_context
def cypher_examples(ctx, output, test):
    """Show curated ASM Cypher examples with suggested columns. Use --test to execute them."""
    examples = [
        {
            "title": "Administrative Users",
            "query": "MATCH (u:User) WHERE u.is_administrator RETURN u LIMIT 10",
            "columns": [{"alias": "u", "property_name": "name"}],
            "notes": "List 10 users with administrative privileges."
        },
        {
            "title": "5 Users",
            "query": "MATCH (u:User) RETURN u LIMIT 5",
            "columns": [{"alias": "u", "property_name": "name"}],
            "notes": "List 5 users in the environment."
        },
        {
            "title": "Ten Machines",
            "query": "MATCH (m:Machine) RETURN m LIMIT 10",
            "columns": [{"alias": "m", "property_name": "name"}],
            "notes": "List 10 machines in the environment."
        },
        {
            "title": "Machine Count",
            "query": "MATCH (m:Machine) RETURN count(m)",
            "columns": [],
            "notes": "Count total number of machines."
        },
        {
            "title": "User Count",
            "query": "MATCH (u:User) RETURN count(u)",
            "columns": [],
            "notes": "Count total number of users."
        },
        {
            "title": "BIMI Eligible Domains",
            "query": "MATCH (dmarc:CloudflareDnsRecord) WHERE dmarc.type = 'TXT' AND dmarc.name ISTARTS WITH '_dmarc.' AND (dmarc.content ICONTAINS 'p=quarantine' OR dmarc.content ICONTAINS 'p=reject') WITH dmarc, REPLACE(dmarc.name, '_dmarc.', '') AS domain OPTIONAL MATCH (spf:CloudflareDnsRecord) WHERE spf.name = domain AND spf.type = 'TXT' AND spf.content ISTARTS WITH 'v=spf1' AND spf.content ICONTAINS '-all' RETURN domain, CASE WHEN dmarc.content ICONTAINS 'p=reject' THEN 'reject' WHEN dmarc.content ICONTAINS 'p=quarantine' THEN 'quarantine' END AS dmarc_policy, CASE WHEN spf IS NOT NULL THEN 'Yes' ELSE 'No' END AS strict_spf, CASE WHEN spf IS NOT NULL AND dmarc.content ICONTAINS 'p=reject' THEN 'Fully Eligible' WHEN spf IS NOT NULL AND dmarc.content ICONTAINS 'p=quarantine' THEN 'Eligible' ELSE 'Partial (no SPF -all)' END AS bimi_eligibility ORDER BY bimi_eligibility DESC, domain ASC",
            "columns": [],
            "notes": "Find domains eligible for BIMI with strict DMARC (quarantine/reject) and SPF hard fail (-all)."
        },
        {
            "title": "Unscanned Workstations and Servers",
            "query": "MATCH (m:Machine) WHERE NOT 'Vulnerability Scanning' IN m.mitigations AND (m.asset_class = 'Workstation' OR m.asset_class = 'Server') RETURN m",
            "columns": [],
            "notes": "Find workstations and servers without vulnerability scanning mitigation."
        }
    ]

    if not test:
        if output == 'json':
            click.echo(json.dumps(examples, indent=2))
            return
        if output == 'plain':
            for i, e in enumerate(examples, 1):
                cmd = f"r7 asm cypher query \"{e['query']}\" --columns '{json.dumps(e['columns'])}'"
                click.echo(f"{i}. {e['title']}")
                click.echo(f"   {e['notes']}")
                click.echo(f"")
                click.echo(f"   {cmd}")
                click.echo()
            return
        if output == 'cmd':
            for e in examples:
                cmd = f"r7 asm cypher query \"{e['query']}\" --columns '{json.dumps(e['columns'])}'"
                click.echo(cmd)
            return
        table = Table(title="ASM Cypher Examples")
        table.add_column("Title", style="cyan")
        table.add_column("Query", style="white")
        table.add_column("Columns (JSON)", style="magenta")
        table.add_column("Notes", style="yellow")
        for e in examples:
            table.add_row(e['title'], e['query'], json.dumps(e['columns']), e['notes'])
        console.print(table)
        return

    client, _ = get_client_and_config(ctx)
    base_url = client.get_base_url('asm')
    url = f"{base_url}?format=json"
    results = []
    for e in examples:
        body = {"columns": e["columns"], "cypher": e["query"]}
        try:
            response = client.make_request("POST", url, data=body)
            if response.status_code != 200:
                results.append({"title": e['title'], "items": 0, "status": f"HTTP {response.status_code}", "notes": e['notes']})
                continue
            data = response.json()
            items = len(data.get('items', [])) if isinstance(data, dict) else 0
            results.append({"title": e['title'], "items": items, "status": "ok" if items > 0 else "no items", "notes": e['notes']})
        except Exception as ex:
            results.append({"title": e['title'], "items": 0, "status": f"error: {ex}", "notes": e['notes']})

    if output == 'json':
        click.echo(json.dumps(results, indent=2))
        return
    if output == 'plain':
        for r in results:
            click.echo(f"- {r['title']}: {r['status']} (items={r['items']})\n  Notes: {r['notes']}")
        return
    table = Table(title="ASM Cypher Examples - Test Results")
    table.add_column("Title", style="cyan")
    table.add_column("Items", style="green")
    table.add_column("Status", style="yellow")
    table.add_column("Notes", style="white")
    for r in results:
        table.add_row(r['title'], str(r['items']), r['status'], r['notes'])
    console.print(table)
# --- End new cypher examples support ---

def _run_surcom_command(ctx, command, args=None):
    """Helper to run surcom SDK commands"""
    try:
        cmd = ['surcom'] + ([command] if command else []) + (list(args) if args else [])
        result = subprocess.run(cmd, text=True)
        if result.returncode != 0:
            ctx.exit(result.returncode)
    except FileNotFoundError:
        console.print("[red]surcom SDK not found. Please install it first: pip install r7-surcom-sdk[/red]")
        ctx.exit(1)
    except Exception as e:
        console.print(f"[red]Error running surcom {command}: {e}[/red]")
        ctx.exit(1)

# --- SDK Integration ---
@asm_group.group(name='sdk')
def sdk_group():
    """Surface Command SDK integration"""
    pass

@sdk_group.command(name='config')
@click.argument('args', nargs=-1)
@click.pass_context
def sdk_config(ctx, args):
    """Configure the surcom-sdk (forwards all extra args)"""
    _run_surcom_command(ctx, 'config', args)

@sdk_group.command(name='connector')
@click.argument('args', nargs=-1)
@click.pass_context  
def sdk_connector(ctx, args):
    """Develop Connectors for the Rapid7 Surface Command Platform"""
    _run_surcom_command(ctx, 'connector', args)

@sdk_group.command(name='type')
@click.argument('args', nargs=-1)
@click.pass_context
def sdk_type(ctx, args):
    """Manage Surface Command Types"""
    _run_surcom_command(ctx, 'type', args)

@sdk_group.command(name='data')
@click.argument('args', nargs=-1)
@click.pass_context
def sdk_data(ctx, args):
    """Interact with Surface Command Data"""
    _run_surcom_command(ctx, 'data', args)

@sdk_group.command(name='help')
@click.argument('args', nargs=-1)
@click.pass_context
def sdk_help(ctx, args):
    """Show surcom SDK help (forwards all extra args)"""
    _run_surcom_command(ctx, '--help', args)

@sdk_group.command(name='version')
@click.argument('args', nargs=-1)
@click.pass_context
def sdk_version(ctx, args):
    """Show surcom SDK version (forwards all extra args)"""
    _run_surcom_command(ctx, '--version', args)
# --- End SDK Integration ---