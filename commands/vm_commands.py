import sys
import json
import re
import click
from rich.console import Console
from rich.table import Table

from utils.config import ConfigManager
from utils.credentials import CredentialManager
from utils.exceptions import APIError, AuthenticationError, ConfigurationError
from api.insightvm_console import InsightVMConsoleClient
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

def _get_vm_console_client(config: ConfigManager) -> InsightVMConsoleClient:
    base_url = config.get('vm_console_url')
    if not base_url:
        raise ConfigurationError("vm_console_url is not set. Use: r7 config set --vm-console-url https://host:3780/api/3")

    vm_user = config.get('vm_username')
    vm_pass = CredentialManager.get_vm_password()
    verify_ssl = bool(config.get('vm_verify_ssl', True))

    if vm_user and vm_pass:
        return InsightVMConsoleClient(base_url, username=vm_user, password=vm_pass, verify_ssl=verify_ssl, timeout=5)
    raise AuthenticationError("VM console credentials missing. Set username via 'r7 config cred vm set-user --username ...' and password via 'r7 config cred vm set-password'.")


def _get_vm_cloud_client(ctx) -> InsightVMCloudClient:
    """Get InsightVM Cloud API client from context"""
    api_key = ctx.obj.get('api_key')
    region = ctx.obj.get('region')
    
    if not api_key:
        raise AuthenticationError("API key required. Set via --api-key or R7_API_KEY env var")
    if not region:
        raise ConfigurationError("Region required. Set via --region")
        
    return InsightVMCloudClient(api_key, region)


@click.group(name='vm')
def vm_group():
    """core vulnerability mgt, console & cloud"""
    pass


@click.group(name='console')
def console_group():
    """InsightVM/Nexpose (Console API v3) commands"""
    pass


# Add console group to vm group
vm_group.add_command(console_group)

# Import and add cloud commands
from commands.vm_cloud_commands import (
    cloud_assets_group, cloud_sites_group, cloud_vulns_group
)
from commands.vm_bulk_export_commands import bulk_export_group

vm_group.add_command(cloud_assets_group)
vm_group.add_command(cloud_sites_group) 
vm_group.add_command(cloud_vulns_group)
vm_group.add_command(bulk_export_group)


@console_group.command(name='config-test')
def vm_config_test():
    """Validate console URL and credentials by calling /sites (fast sanity)."""
    try:
        config = ConfigManager()
        config.validate()
        client = _get_vm_console_client(config)
        data = client.list_sites(page=0, size=1)
        total = len(data.get('resources', [])) if isinstance(data, dict) else 'unknown'
        console.print(f"✅ Console reachable. Sites sample count: {total}")
    except (APIError, AuthenticationError, ConfigurationError) as e:
        click.echo(f"❌ {e}", err=True)


@console_group.group(name='sites')
def sites_group():
    """Manage and view sites"""
    pass


@sites_group.command(name='list')
@click.option('--page', type=int, default=0, show_default=True)
@click.option('--size', type=int, default=200, show_default=True)
@click.option('--output', type=click.Choice(['table', 'json']), help='Output format')
def list_sites(page, size, output):
    """List sites from the console"""
    try:
        config = ConfigManager()
        config.validate()
        client = _get_vm_console_client(config)
        data = client.list_sites(page=page, size=size)
        use_format = determine_output_format(output, config)
        if use_format == 'json':
            click.echo(json.dumps(data, indent=2))
            return
        # table
        resources = data.get('resources', []) if isinstance(data, dict) else []
        table = Table(title=f"Sites (page {page})")
        table.add_column('ID', style='cyan')
        table.add_column('Name', style='white')
        table.add_column('Type', style='yellow')
        table.add_column('Assets', style='green', justify='right')
        table.add_column('Risk', style='magenta', justify='right')
        table.add_column('Importance', style='blue')
        table.add_column('Last Scan', style='white')
        for s in resources:
            risk = s.get('riskScore')
            risk_str = f"{int(risk):,}" if isinstance(risk, (int, float)) else ''
            table.add_row(
                str(s.get('id', '')),
                s.get('name', ''),
                s.get('type', ''),
                str(s.get('assets', '')),
                risk_str,
                s.get('importance', ''),
                s.get('lastScanTime', '')
            )
        console.print(table)
    except (APIError, AuthenticationError, ConfigurationError) as e:
        click.echo(f"❌ {e}", err=True)


@sites_group.command(name='get')
@click.argument('site_id', required=True)
@click.option('--output', type=click.Choice(['table', 'json']), help='Output format')
def get_site(site_id, output):
    """Get a single site by ID"""
    try:
        config = ConfigManager()
        config.validate()
        client = _get_vm_console_client(config)
        data = client.get_site(site_id)
        use_format = determine_output_format(output, config)
        if use_format == 'json':
            click.echo(json.dumps(data, indent=2))
            return
        # table
        table = Table(title=f"Site {data.get('id', site_id)}")
        table.add_column('Field', style='cyan')
        table.add_column('Value', style='white')
        fields = [
            ('id', data.get('id')),
            ('name', data.get('name')),
            ('type', data.get('type')),
            ('description', data.get('description')),
            ('importance', data.get('importance')),
            ('assets', data.get('assets')),
            ('riskScore', data.get('riskScore')),
            ('lastScanTime', data.get('lastScanTime')),
            ('scanEngine', data.get('scanEngine')),
            ('scanTemplate', data.get('scanTemplate')),
        ]
        for label, value in fields:
            if value is not None:
                table.add_row(label, str(value))
        vulns = data.get('vulnerabilities') or {}
        if isinstance(vulns, dict) and vulns:
            table.add_row('vulnerabilities.total', str(vulns.get('total', '')))
            table.add_row('vulnerabilities.critical', str(vulns.get('critical', '')))
            table.add_row('vulnerabilities.severe', str(vulns.get('severe', '')))
            table.add_row('vulnerabilities.moderate', str(vulns.get('moderate', '')))
        console.print(table)
    except (APIError, AuthenticationError, ConfigurationError) as e:
        click.echo(f"❌ {e}", err=True)


# --- Assets ---
@console_group.group(name='assets')
def assets_group():
    """View assets"""
    pass


@assets_group.command(name='list')
@click.option('--page', type=int, default=0, show_default=True)
@click.option('--size', type=int, default=200, show_default=True)
@click.option('--site-id', type=str, help='Limit to assets in a specific site')
@click.option('--hostname', type=str, help='Filter by hostname (case-insensitive substring match)')
@click.option('--output', type=click.Choice(['table', 'json']), help='Output format')
def list_assets(page, size, site_id, hostname, output):
    try:
        config = ConfigManager()
        config.validate()
        client = _get_vm_console_client(config)
        if site_id:
            data = client.list_site_assets(site_id, page=page, size=size)
        else:
            data = client.list_assets(page=page, size=size)
        use_format = determine_output_format(output, config)
        if use_format == 'json':
            click.echo(json.dumps(data, indent=2))
            return
        resources = data.get('resources', []) if isinstance(data, dict) else []
        
        # Apply hostname filter if provided
        if hostname:
            hostname_lower = hostname.lower()
            filtered_resources = []
            for a in resources:
                # Check primary hostname
                host = a.get('hostName', '')
                if host and hostname_lower in host.lower():
                    filtered_resources.append(a)
                    continue
                # Check all hostnames
                hn_list = a.get('hostNames') or []
                if isinstance(hn_list, list):
                    for hn in hn_list:
                        if isinstance(hn, dict) and hn.get('name'):
                            if hostname_lower in hn.get('name', '').lower():
                                filtered_resources.append(a)
                                break
            resources = filtered_resources
        
        table = Table(title=f"Assets - Console API v3 (page {page}) {f'- filtered by hostname: {hostname}' if hostname else ''}")
        table.add_column('ID', style='cyan')
        table.add_column('Host', style='white')
        table.add_column('IPs', style='yellow')
        table.add_column('OS', style='green')
        table.add_column('Risk', style='magenta', justify='right')
        table.add_column('Vulns', style='blue', justify='right')
        
        # Check if we have results after filtering
        if not resources and hostname:
            console.print(f"🔍 No assets found matching hostname filter: '{hostname}'")
            console.print("   Try a different hostname or remove the filter to see all assets")
            return
            
        for a in resources:
            # Hostname
            host = a.get('hostName')
            if not host:
                hn_list = a.get('hostNames') or []
                if isinstance(hn_list, list) and hn_list:
                    host = hn_list[0].get('name')
            # IPs
            ips = []
            if a.get('ip'):
                ips.append(str(a.get('ip')))
            addr_list = a.get('addresses') or []
            if isinstance(addr_list, list):
                ips.extend([str(x.get('ip')) for x in addr_list if x.get('ip')])
            ips_str = ", ".join(list(dict.fromkeys(ips))[:3])
            # OS
            os_text = a.get('os')
            if not os_text:
                fp = a.get('osFingerprint') or {}
                os_text = fp.get('description') or fp.get('product')
            # Risk and vuln counts
            risk = a.get('riskScore')
            risk_str = f"{int(risk):,}" if isinstance(risk, (int, float)) else ''
            vulns = a.get('vulnerabilities') or {}
            vuln_total = vulns.get('total', '') if isinstance(vulns, dict) else ''

            table.add_row(
                str(a.get('id', '')),
                host or '',
                ips_str,
                os_text or '',
                risk_str,
                str(vuln_total)
            )
        console.print(table)
    except (APIError, AuthenticationError, ConfigurationError) as e:
        click.echo(f"❌ {e}", err=True)


@assets_group.command(name='get')
@click.argument('asset_id', required=True)
@click.option('--output', type=click.Choice(['table', 'json']), help='Output format')
def get_asset(asset_id, output):
    try:
        config = ConfigManager()
        config.validate()
        client = _get_vm_console_client(config)
        data = client.get_asset(asset_id)
        use_format = determine_output_format(output, config)
        if use_format == 'json':
            click.echo(json.dumps(data, indent=2))
            return
        table = Table(title=f"Asset {data.get('id', asset_id)}")
        table.add_column('Field', style='cyan')
        table.add_column('Value', style='white')
        # Core identifiers
        host = data.get('hostName')
        if not host and isinstance(data.get('hostNames'), list) and data['hostNames']:
            host = data['hostNames'][0].get('name')
        # All hostnames (if present)
        hostnames = []
        if isinstance(data.get('hostNames'), list):
            hostnames = [hn.get('name') for hn in data['hostNames'] if hn.get('name')]
            # dedupe & sort for stability
            hostnames = sorted(set(hostnames))
        ips = []
        if data.get('ip'):
            ips.append(str(data.get('ip')))
        if isinstance(data.get('addresses'), list):
            ips.extend([x.get('ip') for x in data['addresses'] if x.get('ip')])
        # dedupe & sort for readability
        ips = sorted(set([i for i in ips if i]))
        macs = []
        if data.get('mac'):
            macs.append(str(data.get('mac')))
        if isinstance(data.get('addresses'), list):
            macs.extend([x.get('mac') for x in data['addresses'] if x.get('mac')])
        macs = sorted(set([m for m in macs if m]))
        os_text = data.get('os')
        if not os_text and isinstance(data.get('osFingerprint'), dict):
            ofp = data['osFingerprint']
            os_text = ofp.get('description') or ofp.get('product')

        fields = [
            ('id', data.get('id')),
            ('hostName', host),
            ('hostNames', ", ".join(hostnames[:10]) if hostnames else None),
            ('ips', ", ".join(ips[:10])),
            ('macs', ", ".join(macs[:10])),
            ('os', os_text),
            ('riskScore', data.get('riskScore')),
            ('rawRiskScore', data.get('rawRiskScore')),
        ]
        for label, value in fields:
            if value:
                table.add_row(label, str(value))
        vulns = data.get('vulnerabilities') or {}
        if isinstance(vulns, dict) and vulns:
            table.add_row('vulnerabilities.total', str(vulns.get('total', '')))
            table.add_row('vulnerabilities.critical', str(vulns.get('critical', '')))
            table.add_row('vulnerabilities.severe', str(vulns.get('severe', '')))
            table.add_row('vulnerabilities.moderate', str(vulns.get('moderate', '')))
            # Include additional useful indicators when present
            if 'exploits' in vulns:
                table.add_row('vulnerabilities.exploits', str(vulns.get('exploits', '')))
            if 'malwareKits' in vulns:
                table.add_row('vulnerabilities.malwareKits', str(vulns.get('malwareKits', '')))
        console.print(table)
    except (APIError, AuthenticationError, ConfigurationError) as e:
        click.echo(f"❌ {e}", err=True)


@assets_group.command(name='delete')
@click.argument('asset_id', required=True)
@click.option('--confirm', is_flag=True, help='Skip confirmation prompt')
@click.option('--output', type=click.Choice(['table', 'json']), help='Output format')
def delete_asset(asset_id, confirm, output):
    """Delete an asset from InsightVM console"""
    try:
        config = ConfigManager()
        config.validate()
        client = _get_vm_console_client(config)
        
        # Get asset info first for confirmation
        if not confirm:
            try:
                asset_data = client.get_asset(asset_id)
                asset_name = asset_data.get('hostName') or 'Unknown'
                if not click.confirm(f"Are you sure you want to delete asset '{asset_name}' (ID: {asset_id})?"):
                    click.echo("❌ Delete cancelled")
                    return
            except Exception:
                # If we can't get asset info, still ask for confirmation
                if not click.confirm(f"Are you sure you want to delete asset ID: {asset_id}?"):
                    click.echo("❌ Delete cancelled")
                    return
        
        # Perform the deletion
        result = client.delete_asset(asset_id)
        
        use_format = determine_output_format(output, config)
        if use_format == 'json':
            click.echo(json.dumps(result, indent=2))
        else:
            console.print(f"[green]✅ Successfully deleted asset {asset_id}[/green]")
            
    except (APIError, AuthenticationError, ConfigurationError) as e:
        click.echo(f"❌ {e}", err=True)


# --- Vulnerabilities (definitions) ---
@console_group.group(name='vulns')
def vulns_group():
    """Vulnerability definitions"""
    pass


@vulns_group.command(name='list')
@click.option('--page', type=int, default=0, show_default=True)
@click.option('--size', type=int, default=200, show_default=True)
@click.option('--severity', type=str, help='Filter by severity (e.g., Critical, Severe, Moderate, Low)')
@click.option('--cve', type=str, help='Filter by CVE substring (case-insensitive)')
@click.option('--output', type=click.Choice(['table', 'json']), help='Output format')
def list_vulns(page, size, severity, cve, output):
    try:
        config = ConfigManager()
        config.validate()
        client = _get_vm_console_client(config)
        data = client.list_vulnerabilities(page=page, size=size)
        use_format = determine_output_format(output, config)
        if use_format == 'json':
            click.echo(json.dumps(data, indent=2))
            return
        resources = data.get('resources', []) if isinstance(data, dict) else []
        # Apply simple client-side filters
        if severity:
            resources = [v for v in resources if str(v.get('severity', '')).lower() == severity.lower()]
        if cve:
            cv = cve.lower()
            def _has_cve(v):
                cves = v.get('cves') or []
                return any(cv in (c or '').lower() for c in cves)
            resources = [v for v in resources if _has_cve(v)]
        table = Table(title=f"Vulnerabilities (page {page})")
        table.add_column('ID', style='cyan')
        table.add_column('Title', style='white')
        table.add_column('Severity', style='magenta', justify='right')
        table.add_column('CVEs', style='yellow')
        for v in resources:
            cves = v.get('cves') or []
            if isinstance(cves, list):
                cve_str = ", ".join(cves[:3])
            else:
                cve_str = ''
            table.add_row(
                str(v.get('id', '')),
                v.get('title', v.get('name', '')),
                str(v.get('severity', '')),
                cve_str,
            )
        console.print(table)
    except (APIError, AuthenticationError, ConfigurationError) as e:
        click.echo(f"❌ {e}", err=True)


@vulns_group.command(name='get')
@click.argument('vuln_id', required=True)
@click.option('--output', type=click.Choice(['table', 'json']), help='Output format')
def get_vuln(vuln_id, output):
    try:
        config = ConfigManager()
        config.validate()
        client = _get_vm_console_client(config)
        data = client.get_vulnerability(vuln_id)
        use_format = determine_output_format(output, config)
        if use_format == 'json':
            click.echo(json.dumps(data, indent=2))
            return
        table = Table(title=f"Vulnerability {data.get('id', vuln_id)}")
        table.add_column('Field', style='cyan')
        table.add_column('Value', style='white')
        # Clean description: prefer plain text, strip HTML if needed
        desc_val = data.get('description')
        desc_clean = None
        if isinstance(desc_val, dict):
            desc_clean = desc_val.get('text')
            if not desc_clean and isinstance(desc_val.get('html'), str):
                # naive HTML tag strip for readability
                desc_clean = re.sub(r"<[^>]+>", "", desc_val.get('html'))
        elif isinstance(desc_val, str):
            desc_clean = desc_val
        fields = [
            ('id', data.get('id')),
            ('title', data.get('title') or data.get('name')),
            ('severity', data.get('severity')),
            ('description', desc_clean),
        ]
        for label, v in fields:
            if v is not None:
                table.add_row(label, str(v))
        cves = data.get('cves') or []
        if cves:
            table.add_row('cves', ", ".join(cves[:10]))
        console.print(table)
    except (APIError, AuthenticationError, ConfigurationError) as e:
        click.echo(f"❌ {e}", err=True)


# --- Findings (per-asset vulnerabilities) ---
@console_group.group(name='findings')
def findings_group():
    """Per-asset findings"""
    pass


@findings_group.command(name='asset')
@click.argument('asset_id', required=True)
@click.option('--page', type=int, default=0, show_default=True)
@click.option('--size', type=int, default=200, show_default=True)
@click.option('--status', type=str, help='Filter by finding status (e.g., vulnerable, vulnerable-potential, not-vulnerable)')
@click.option('--id-contains', 'id_contains', type=str, help='Filter by vulnerability id substring (case-insensitive)')
@click.option('--port', type=int, help='Filter by port present in results')
@click.option('--protocol', type=str, help='Filter by protocol present in results (e.g., tcp, udp)')
@click.option('--details/--no-details', default=False, help='Fetch vulnerability details to enrich Title/Severity')
@click.option('--output', type=click.Choice(['table', 'json']), help='Output format')
def list_asset_findings(asset_id, page, size, status, id_contains, port, protocol, details, output):
    try:
        config = ConfigManager()
        config.validate()
        client = _get_vm_console_client(config)
        data = client.list_asset_vulnerabilities(asset_id, page=page, size=size)
        use_format = determine_output_format(output, config)
        if use_format == 'json':
            click.echo(json.dumps(data, indent=2))
            return
        resources = data.get('resources', []) if isinstance(data, dict) else []
        # Apply filters locally
        def _matches_filters(f):
            if status and str(f.get('status', '')).lower() != status.lower():
                return False
            if id_contains and id_contains.lower() not in str(f.get('id', '')).lower():
                return False
            if port is not None:
                res = f.get('results') or []
                if not any(r.get('port') == port for r in res if isinstance(r, dict)):
                    return False
            if protocol:
                res = f.get('results') or []
                if not any(str(r.get('protocol', '')).lower() == protocol.lower() for r in res if isinstance(r, dict)):
                    return False
            return True
        resources = [f for f in resources if _matches_filters(f)]

        # Optionally enrich with vulnerability definitions
        details_map = {}
        if details and resources:
            config = ConfigManager()
            config.validate()
            client = _get_vm_console_client(config)
            for f in resources:
                vid = str(f.get('id'))
                if not vid:
                    continue
                try:
                    vdef = client.get_vulnerability(vid)
                    details_map[vid] = vdef
                except Exception:
                    details_map[vid] = {}

        # Adjust table columns based on whether details are being fetched
        if details:
            table = Table(title=f"Asset {asset_id} Findings (page {page}) - with details")
            table.add_column('Vuln ID', style='cyan')
            table.add_column('Title', style='white')
            table.add_column('Severity', style='magenta', justify='right')
            table.add_column('Status', style='green')
            table.add_column('Instances', style='blue', justify='right')
            table.add_column('Since', style='white')
        else:
            table = Table(title=f"Asset {asset_id} Findings (page {page})")
            table.add_column('Vuln ID', style='cyan')
            table.add_column('Status', style='green')
            table.add_column('Instances', style='blue', justify='right')
            table.add_column('Since', style='white')

        # Check if we have any results to display
        if not resources:
            # Get asset info for context
            try:
                asset_data = client.get_asset(asset_id)
                asset_name = None
                if isinstance(asset_data, dict):
                    hostnames = asset_data.get('hostNames', [])
                    if hostnames and isinstance(hostnames, list):
                        asset_name = hostnames[0].get('name', '')
                    if not asset_name:
                        asset_name = asset_data.get('ip', f'Asset {asset_id}')
                
                # Check if filters were applied
                filters_applied = any([status, id_contains, port, protocol])
                if filters_applied:
                    console.print(f"🔍 No findings found for {asset_name} (ID: {asset_id}) matching the specified filters")
                    console.print("   Try removing some filters or check different assets")
                else:
                    console.print(f"✅ No vulnerabilities found for {asset_name} (ID: {asset_id})")
                    console.print("   This asset appears to be clean - no security findings detected")
                return
            except Exception:
                # Fallback if we can't get asset details
                filters_applied = any([status, id_contains, port, protocol])
                if filters_applied:
                    console.print(f"🔍 No findings found for asset {asset_id} matching the specified filters")
                else:
                    console.print(f"✅ No vulnerabilities found for asset {asset_id}")
                return
            
        for f in resources:
            vid = str(f.get('id', f.get('vulnerabilityId', '')))
            instances = f.get('instances') if isinstance(f.get('instances'), int) else ''
            since = f.get('since', '')
            status = f.get('status', '')
            
            if details:
                vdef = details_map.get(vid, {})
                title = vdef.get('title') or vdef.get('name') or 'N/A'
                severity = vdef.get('severity') or 'N/A'
                table.add_row(
                    vid,
                    title,
                    str(severity),
                    status,
                    str(instances),
                    since,
                )
            else:
                table.add_row(
                    vid,
                    status,
                    str(instances),
                    since,
                )
        
        console.print(table)
        
        # Add helpful summary message
        total_findings = len(resources)
        console.print(f"\n[dim]Found {total_findings} finding(s) for asset {asset_id}[/dim]")
    except (APIError, AuthenticationError, ConfigurationError) as e:
        click.echo(f"❌ {e}", err=True)


# --- Scans ---
@console_group.group(name='scans')
def scans_group():
    """View scans"""
    pass


@scans_group.command(name='list')
@click.option('--page', type=int, default=0, show_default=True)
@click.option('--size', type=int, default=200, show_default=True)
@click.option('--site-id', type=str, help='Limit to scans in a specific site')
@click.option('--output', type=click.Choice(['table', 'json']), help='Output format')
def list_scans(page, size, site_id, output):
    try:
        config = ConfigManager()
        config.validate()
        client = _get_vm_console_client(config)
        if site_id:
            data = client.list_site_scans(site_id, page=page, size=size)
        else:
            data = client.list_scans(page=page, size=size)
        use_format = determine_output_format(output, config)
        if use_format == 'json':
            click.echo(json.dumps(data, indent=2))
            return
        resources = data.get('resources', []) if isinstance(data, dict) else []
        table = Table(title=f"Scans (page {page})")
        table.add_column('ID', style='cyan')
        table.add_column('Scan Name', style='white')
        table.add_column('Site', style='yellow')
        table.add_column('Status', style='green')
        table.add_column('Start', style='white')
        table.add_column('End', style='white')
        for s in resources:
            table.add_row(
                str(s.get('id', '')),
                s.get('scanName', ''),
                s.get('siteName', str(s.get('siteId', ''))),
                s.get('status', ''),
                s.get('startTime', ''),
                s.get('endTime', '')
            )
        console.print(table)
    except (APIError, AuthenticationError, ConfigurationError) as e:
        click.echo(f"❌ {e}", err=True)


@scans_group.command(name='get')
@click.argument('scan_id', required=True)
@click.option('--output', type=click.Choice(['table', 'json']), help='Output format')
def get_scan(scan_id, output):
    try:
        config = ConfigManager()
        config.validate()
        client = _get_vm_console_client(config)
        data = client.get_scan(scan_id)
        use_format = determine_output_format(output, config)
        if use_format == 'json':
            click.echo(json.dumps(data, indent=2))
            return
        table = Table(title=f"Scan {data.get('id', scan_id)}")
        table.add_column('Field', style='cyan')
        table.add_column('Value', style='white')
        # Prepare engineIds as a comma-separated string if present
        engine_ids_val = None
        if isinstance(data.get('engineIds'), list) and data['engineIds']:
            engine_ids_val = ", ".join(str(eid) for eid in data['engineIds'])

        fields = [
            ('id', data.get('id')),
            ('scanName', data.get('scanName')),
            ('siteId', data.get('siteId')),
            ('siteName', data.get('siteName')),
            ('status', data.get('status')),
            ('assets', data.get('assets')),
            ('scanType', data.get('scanType')),
            ('startedByUsername', data.get('startedByUsername')),
            ('engineId', data.get('engineId')),
            ('engineName', data.get('engineName')),
            ('engineIds', engine_ids_val),
            ('startTime', data.get('startTime')),
            ('endTime', data.get('endTime')),
            ('duration', data.get('duration')),
        ]
        for label, v in fields:
            if v is not None:
                table.add_row(label, str(v))
        vulns = data.get('vulnerabilities') or {}
        if isinstance(vulns, dict) and vulns:
            table.add_row('vulnerabilities.total', str(vulns.get('total', '')))
            table.add_row('vulnerabilities.critical', str(vulns.get('critical', '')))
            table.add_row('vulnerabilities.severe', str(vulns.get('severe', '')))
            table.add_row('vulnerabilities.moderate', str(vulns.get('moderate', '')))
        console.print(table)
    except (APIError, AuthenticationError, ConfigurationError) as e:
        click.echo(f"❌ {e}", err=True)


@scans_group.command(name='start')
@click.argument('site_id', required=True, type=int)
@click.option('--name', type=str, help='Scan name')
@click.option('--template-id', type=str, help='Scan template ID')
@click.option('--engine-id', type=int, help='Scan engine ID')
@click.option('--hosts', type=str, multiple=True, help='Specific hosts to scan (can be used multiple times)')
@click.option('--asset-group-ids', type=str, help='Comma-separated asset group IDs')
@click.option('--override-blackout/--no-override-blackout', default=False, help='Override scan blackout window')
@click.option('--output', type=click.Choice(['table', 'json']), default='table', help='Output format')
def start_scan(site_id, name, template_id, engine_id, hosts, asset_group_ids, override_blackout, output):
    """Start a scan for the specified site"""
    try:
        config = ConfigManager()
        config.validate()
        client = _get_vm_console_client(config)
        
        # Parse asset group IDs
        asset_groups = None
        if asset_group_ids:
            try:
                asset_groups = [int(x.strip()) for x in asset_group_ids.split(',')]
            except ValueError:
                click.echo("❌ Invalid asset group IDs format. Use comma-separated integers.", err=True)
                return
        
        # Convert hosts tuple to list
        host_list = list(hosts) if hosts else None
        
        data = client.start_site_scan(
            site_id=site_id,
            scan_name=name,
            template_id=template_id,
            engine_id=engine_id,
            hosts=host_list,
            asset_group_ids=asset_groups,
            override_blackout=override_blackout
        )
        
        use_format = determine_output_format(output, config)
        if use_format == 'json':
            click.echo(json.dumps(data, indent=2))
            return
        
        # Success message
        scan_id = data.get('id')
        if scan_id:
            console.print("✅ Scan started successfully!")
            console.print(f"Scan ID: {scan_id}")
            console.print(f"Site ID: {site_id}")
            if name:
                console.print(f"Scan Name: {name}")
        else:
            console.print("✅ Scan start request submitted")
            
    except (APIError, AuthenticationError, ConfigurationError) as e:
        click.echo(f"❌ {e}", err=True)


@scans_group.command(name='stop')
@click.argument('scan_id', required=True, type=int)
@click.option('--output', type=click.Choice(['table', 'json']), default='table', help='Output format')
def stop_scan(scan_id, output):
    """Stop a running scan"""
    try:
        config = ConfigManager()
        config.validate()
        client = _get_vm_console_client(config)
        
        data = client.update_scan_status(scan_id, 'stop')
        
        use_format = determine_output_format(output, config)
        if use_format == 'json':
            click.echo(json.dumps(data, indent=2))
            return
        
        console.print(f"✅ Scan {scan_id} stop request sent")
        
    except (APIError, AuthenticationError, ConfigurationError) as e:
        click.echo(f"❌ {e}", err=True)


@scans_group.command(name='pause')
@click.argument('scan_id', required=True, type=int)
@click.option('--output', type=click.Choice(['table', 'json']), default='table', help='Output format')
def pause_scan(scan_id, output):
    """Pause a running scan"""
    try:
        config = ConfigManager()
        config.validate()
        client = _get_vm_console_client(config)
        
        data = client.update_scan_status(scan_id, 'pause')
        
        use_format = determine_output_format(output, config)
        if use_format == 'json':
            click.echo(json.dumps(data, indent=2))
            return
        
        console.print(f"✅ Scan {scan_id} pause request sent")
        
    except (APIError, AuthenticationError, ConfigurationError) as e:
        click.echo(f"❌ {e}", err=True)


@scans_group.command(name='resume')
@click.argument('scan_id', required=True, type=int)
@click.option('--output', type=click.Choice(['table', 'json']), default='table', help='Output format')
def resume_scan(scan_id, output):
    """Resume a paused scan"""
    try:
        config = ConfigManager()
        config.validate()
        client = _get_vm_console_client(config)
        
        data = client.update_scan_status(scan_id, 'resume')
        
        use_format = determine_output_format(output, config)
        if use_format == 'json':
            click.echo(json.dumps(data, indent=2))
            return
        
        console.print(f"✅ Scan {scan_id} resume request sent")
        
    except (APIError, AuthenticationError, ConfigurationError) as e:
        click.echo(f"❌ {e}", err=True)