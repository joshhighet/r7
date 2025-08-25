import sys
import json
import click
from rich.console import Console
from rich.table import Table

from utils.config import ConfigManager
from utils.credentials import CredentialManager
from utils.exceptions import APIError, AuthenticationError, ConfigurationError
from api.insightvm_cloud import InsightVMCloudClient

console = Console()

def determine_output_format(output, config):
    """Determine output format with pipe detection"""
    if output:
        return output
    elif not sys.stdout.isatty():
        return 'json'
    else:
        return config.get('default_output', 'table')


def _get_vm_cloud_client(ctx) -> InsightVMCloudClient:
    """Get InsightVM Cloud API client from context"""
    # First try context (CLI flags)
    api_key = ctx.obj.get('api_key')
    region = ctx.obj.get('region')
    
    # If not in context, get from config
    if not api_key or not region:
        config = ConfigManager()
        if not api_key:
            api_key = CredentialManager.get_api_key()
        if not region:
            region = config.get('region', 'us')
    
    if not api_key:
        raise AuthenticationError("API key required. Set via 'r7 config cred set' or --api-key flag")
    if not region:
        raise ConfigurationError("Region required. Set via 'r7 config set --region' or --region flag")
        
    return InsightVMCloudClient(api_key, region)


def _get_tenant_prefix() -> str:
    """Get the VM tenant prefix from config"""
    try:
        config = ConfigManager()
        return config.get('vm_tenant_prefix', '')
    except Exception:
        return ''


def _detect_and_set_tenant_prefix(assets: list) -> str:
    """Detect common tenant prefix from asset IDs and auto-set it"""
    if not assets or len(assets) < 2:
        return ''
    
    # Get first few asset IDs to analyze
    asset_ids = [asset.get('id', '') for asset in assets[:10] if asset.get('id')]
    if len(asset_ids) < 2:
        return ''
    
    # Find longest common prefix ending with '-'
    common_prefix = ''
    first_id = asset_ids[0]
    
    for i in range(len(first_id)):
        char = first_id[i]
        if all(len(aid) > i and aid[i] == char for aid in asset_ids):
            common_prefix += char
        else:
            break
    
    # Find the last dash in the common prefix to get tenant prefix
    last_dash = common_prefix.rfind('-')
    if last_dash > 20:  # Must be substantial prefix
        tenant_prefix = common_prefix[:last_dash + 1]
        
        # Auto-save to config if not already set
        try:
            config = ConfigManager()
            current_prefix = config.get('vm_tenant_prefix', '')
            if not current_prefix and tenant_prefix:
                config.update({'vm_tenant_prefix': tenant_prefix})
                config.save_config()
                console.print(f"[dim]Auto-detected and saved tenant prefix: {tenant_prefix[:30]}...[/dim]")
        except Exception:
            pass
            
        return tenant_prefix
    
    return ''


def _expand_asset_id(asset_id: str) -> str:
    """Expand a short asset ID to full ID using tenant prefix"""
    tenant_prefix = _get_tenant_prefix()
    if tenant_prefix and not asset_id.startswith(tenant_prefix):
        return tenant_prefix + asset_id
    return asset_id


def _shorten_asset_id(asset_id: str) -> str:
    """Shorten a full asset ID by removing tenant prefix"""
    tenant_prefix = _get_tenant_prefix()
    if tenant_prefix and asset_id.startswith(tenant_prefix):
        return asset_id[len(tenant_prefix):]
    return asset_id


# --- Cloud Assets ---
@click.group(name='assets')
def cloud_assets_group():
    """Manage assets (Cloud API v4)"""
    pass


@cloud_assets_group.command(name='list')
@click.option('--cursor', type=str, help='Cursor for pagination')
@click.option('--size', type=int, default=50, show_default=True, help='Number of assets per page')
@click.option('--site-id', type=str, help='Filter by site ID')
@click.option('--asset-id', type=str, help='Filter by specific asset ID')
@click.option('--hostname', type=str, help='Filter by hostname (case-insensitive substring match)')
@click.option('--output', type=click.Choice(['table', 'json']), help='Output format')
@click.pass_context
def list_cloud_assets(ctx, cursor, size, site_id, asset_id, hostname, output):
    """List assets using Cloud API v4"""
    try:
        client = _get_vm_cloud_client(ctx)
        
        # Build filters
        site_ids = [site_id] if site_id else None
        asset_ids = [asset_id] if asset_id else None
        
        data = client.search_assets(
            cursor=cursor,
            size=size,
            site_ids=site_ids,
            asset_ids=asset_ids
        )
        
        config = ConfigManager()
        use_format = determine_output_format(output, config)
        if use_format == 'json':
            click.echo(json.dumps(data, indent=2))
            return
            
        # Table output
        assets = data.get('data', [])
        metadata = data.get('metadata', {})
        
        # Auto-detect tenant prefix if not set and we have assets
        tenant_prefix = _get_tenant_prefix()
        if not tenant_prefix and assets:
            tenant_prefix = _detect_and_set_tenant_prefix(assets)
        
        # Apply hostname filter if provided
        if hostname:
            hostname_lower = hostname.lower()
            assets = [asset for asset in assets 
                     if hostname_lower in (asset.get('host_name', '') or '').lower()]
        
        table = Table(title=f"Assets - Cloud API v4 (page {metadata.get('number', 0)}) {f'- filtered by hostname: {hostname}' if hostname else ''}")
        table.add_column('ID', style='cyan')
        table.add_column('Hostname', style='white')
        table.add_column('IP Address', style='yellow')
        table.add_column('OS', style='green')
        table.add_column('Risk Score', style='magenta', justify='right')
        table.add_column('Critical Vulns', style='red', justify='right')
        table.add_column('Last Assessed', style='white')
        
        # Check if we have results after filtering
        if not assets and hostname:
            console.print(f"🔍 No assets found matching hostname filter: '{hostname}'")
            console.print("   Try a different hostname or remove the filter to see all assets")
            return
            
        for asset in assets:
            # Extract asset details - using correct field names from API
            asset_id_val = asset.get('id', '')
            hostname = asset.get('host_name', '')  # Changed from hostName
            ip_address = asset.get('ip', '')
            os_desc = asset.get('os_description', '')  # More detailed than os_name
            risk_score = asset.get('risk_score', 0)  # Changed from riskScore
            
            # Extract vulnerability counts - using correct field names
            critical_vulns = asset.get('critical_vulnerabilities', 0)
            
            last_assessed = asset.get('last_assessed_for_vulnerabilities', '')
            
            # Use tenant prefix shortening for display (refresh prefix in case it was auto-detected)
            if tenant_prefix:
                short_id = asset_id_val[len(tenant_prefix):] if asset_id_val.startswith(tenant_prefix) else asset_id_val
            else:
                short_id = asset_id_val
            
            table.add_row(
                short_id,
                hostname,
                ip_address,
                os_desc,
                f"{risk_score:,.1f}" if isinstance(risk_score, (int, float)) else str(risk_score),
                str(critical_vulns),
                last_assessed
            )
        
        console.print(table)
        
        # Show tenant prefix info if configured
        if tenant_prefix:
            console.print(f"\n[dim]Short IDs shown (tenant prefix: {tenant_prefix[:20]}...) - Use 'r7 vm assets get <short-id>' to query assets[/dim]")
        else:
            console.print(f"\n[dim]Tenant prefix will be auto-detected and saved on first run with multiple assets[/dim]")
        
        # Show pagination info
        if metadata.get('cursor'):
            console.print(f"[dim]Next page cursor: {metadata['cursor']}[/dim]")
        if metadata.get('totalResources'):
            console.print(f"[dim]Total resources: {metadata['totalResources']:,}[/dim]")
            
    except (APIError, AuthenticationError, ConfigurationError) as e:
        click.echo(f"❌ {e}", err=True)


@cloud_assets_group.command(name='get')
@click.argument('asset_id', required=True)
@click.option('--output', type=click.Choice(['table', 'json']), help='Output format')
@click.pass_context
def get_cloud_asset(ctx, asset_id, output):
    """Get asset details by ID using Cloud API v4
    
    Supports short IDs when tenant prefix is configured.
    Example: 'r7 vm assets get asset-123' (short) or full ID
    """
    try:
        client = _get_vm_cloud_client(ctx)
        
        # Expand short ID to full ID if needed
        full_asset_id = _expand_asset_id(asset_id)
        
        data = client.get_asset(full_asset_id)
        
        config = ConfigManager()
        use_format = determine_output_format(output, config)
        if use_format == 'json':
            click.echo(json.dumps(data, indent=2))
            return
            
        # Table output - asset data is returned directly, not wrapped
        asset = data if not isinstance(data, dict) or 'data' not in data else data.get('data', {})
        
        # Show short ID in title if we have tenant prefix configured
        display_id = _shorten_asset_id(asset.get('id', full_asset_id))
        table = Table(title=f"Asset Details - {display_id}")
        table.add_column('Field', style='cyan')
        table.add_column('Value', style='white')
        
        # Core asset information - using correct field names from API
        fields = [
            ('ID', asset.get('id')),
            ('Hostname', asset.get('host_name')),
            ('IP Address', asset.get('ip')),
            ('MAC Address', asset.get('mac')),
            ('OS Description', asset.get('os_description')),
            ('OS Family', asset.get('os_family')),
            ('OS Version', asset.get('os_version')),
            ('Risk Score', asset.get('risk_score')),
            ('Last Assessed', asset.get('last_assessed_for_vulnerabilities')),
            ('Last Scan Start', asset.get('last_scan_start')),
            ('Last Scan End', asset.get('last_scan_end')),
            ('Exploits', asset.get('exploits')),
            ('Malware Kits', asset.get('malware_kits'))
        ]
        
        for label, value in fields:
            if value is not None:
                if label == 'Risk Score' and isinstance(value, (int, float)):
                    table.add_row(label, f"{value:,.1f}")
                else:
                    table.add_row(label, str(value))
        
        # Vulnerability summary - using correct field names
        crit = asset.get('critical_vulnerabilities')
        sev = asset.get('severe_vulnerabilities')
        mod = asset.get('moderate_vulnerabilities')
        if crit is not None or sev is not None or mod is not None:
            table.add_row('', '')  # Separator
            table.add_row('[bold]Vulnerabilities', '')
            if crit is not None:
                table.add_row('Critical', str(crit))
            if sev is not None:
                table.add_row('Severe', str(sev))
            if mod is not None:
                table.add_row('Moderate', str(mod))
        
        console.print(table)
        
    except (APIError, AuthenticationError, ConfigurationError) as e:
        click.echo(f"❌ {e}", err=True)


# --- Cloud Sites ---
@click.group(name='sites')
def cloud_sites_group():
    """Manage sites (Cloud API v4)"""
    pass


@cloud_sites_group.command(name='list')
@click.option('--cursor', type=str, help='Cursor for pagination')
@click.option('--page', type=int, help='Page number (alternative to cursor)')
@click.option('--size', type=int, default=50, show_default=True, help='Number of sites per page')
@click.option('--details/--no-details', default=False, help='Include detailed site information')
@click.option('--output', type=click.Choice(['table', 'json']), help='Output format')
@click.pass_context
def list_cloud_sites(ctx, cursor, page, size, details, output):
    """List sites using Cloud API v4"""
    try:
        client = _get_vm_cloud_client(ctx)
        
        data = client.get_sites(
            cursor=cursor,
            page=page,
            size=size,
            include_details=details
        )
        
        config = ConfigManager()
        use_format = determine_output_format(output, config)
        if use_format == 'json':
            click.echo(json.dumps(data, indent=2))
            return
            
        # Table output
        sites = data.get('data', [])
        metadata = data.get('metadata', {})
        
        table = Table(title=f"Sites - Cloud API v4 (page {metadata.get('number', 0)})")
        table.add_column('Name', style='white')
        table.add_column('Type', style='yellow')
        
        # The API returns minimal site data - just name and type
        for site in sites:
            name = site.get('name', '')
            site_type = site.get('type', '')
            
            table.add_row(
                name,
                site_type
            )
        
        console.print(table)
        
        # Show pagination info
        if metadata.get('cursor'):
            console.print(f"\n[dim]Next page cursor: {metadata['cursor']}[/dim]")
        if metadata.get('totalResources'):
            console.print(f"[dim]Total resources: {metadata['totalResources']:,}[/dim]")
            
    except (APIError, AuthenticationError, ConfigurationError) as e:
        click.echo(f"❌ {e}", err=True)



# --- Cloud Vulnerabilities ---
@click.group(name='vulns')
def cloud_vulns_group():
    """Search vulnerabilities (Cloud API v4)"""
    pass


@cloud_vulns_group.command(name='list')
@click.option('--cursor', type=str, help='Cursor for pagination')
@click.option('--size', type=int, default=50, show_default=True, help='Number of vulnerabilities per page')
@click.option('--site-id', type=str, help='Filter by site ID')
@click.option('--asset-id', type=str, help='Filter by asset ID')
@click.option('--vuln-id', type=str, help='Filter by vulnerability ID')
@click.option('--output', type=click.Choice(['table', 'json']), help='Output format')
@click.pass_context
def list_cloud_vulns(ctx, cursor, size, site_id, asset_id, vuln_id, output):
    """Search vulnerabilities using Cloud API v4"""
    try:
        client = _get_vm_cloud_client(ctx)
        
        # Build filters
        site_ids = [site_id] if site_id else None
        asset_ids = [asset_id] if asset_id else None
        vuln_ids = [vuln_id] if vuln_id else None
        
        data = client.search_vulnerabilities(
            cursor=cursor,
            size=size,
            site_ids=site_ids,
            asset_ids=asset_ids,
            vuln_ids=vuln_ids
        )
        
        config = ConfigManager()
        use_format = determine_output_format(output, config)
        if use_format == 'json':
            click.echo(json.dumps(data, indent=2))
            return
            
        # Table output
        vulns = data.get('data', [])
        metadata = data.get('metadata', {})
        
        table = Table(title=f"Vulnerabilities - Cloud API v4 (page {metadata.get('number', 0)})")
        table.add_column('ID', style='cyan')
        table.add_column('CVE', style='yellow') 
        table.add_column('Description', style='white', max_width=50)
        table.add_column('CVSS v3', style='magenta', justify='right')
        table.add_column('Added', style='green')
        table.add_column('Categories', style='white', max_width=30)
        
        for vuln in vulns:
            vuln_id = vuln.get('id', '')
            cves = vuln.get('cves', '')
            description = vuln.get('description', '')[:100] + '...' if len(vuln.get('description', '')) > 100 else vuln.get('description', '')
            cvss_v3 = vuln.get('cvss_v3_score', '')
            added = vuln.get('added', '')
            categories = vuln.get('categories', '')
            
            table.add_row(
                str(vuln_id),
                str(cves),
                description,
                f"{cvss_v3:.1f}" if isinstance(cvss_v3, (int, float)) else str(cvss_v3),
                added[:10] if added else '',  # Just date part
                categories[:30] + '...' if len(categories) > 30 else categories
            )
        
        console.print(table)
        
        # Show pagination info
        if metadata.get('cursor'):
            console.print(f"\n[dim]Next page cursor: {metadata['cursor']}[/dim]")
        if metadata.get('totalResources'):
            console.print(f"[dim]Total resources: {metadata['totalResources']:,}[/dim]")
            
    except (APIError, AuthenticationError, ConfigurationError) as e:
        click.echo(f"❌ {e}", err=True)