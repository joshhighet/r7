import sys
import click
import json
from datetime import datetime
from rich.console import Console
from rich.table import Table
from rich.panel import Panel
from rich.progress import Progress, SpinnerColumn, TextColumn, TimeElapsedColumn
from utils.config import ConfigManager
from utils.credentials import CredentialManager
from utils.cache import CacheManager
from api.client import Rapid7Client
from utils.exceptions import AuthenticationError

console = Console()

def determine_output_format(output, config):
    """Determine output format with pipe detection"""
    if output:
        return output
    elif not sys.stdout.isatty():
        return 'json'
    else:
        return config.get('default_output', 'simple')

# ============================================================================
# Helper Functions
# ============================================================================

def get_client(ctx, no_cache=False):
    """Initialize and return Rapid7 client with credentials"""
    config_manager = ConfigManager()
    config_manager.validate()
    
    api_key = CredentialManager.get_api_key(ctx.obj.get('api_key'))
    if not api_key:
        raise AuthenticationError("API key not found. Use 'r7 config cred store' to save credentials.")
    
    region = ctx.obj.get('region') or config_manager.get('region', 'us')
    
    cache_manager = None
    if config_manager.get('cache_enabled') and not no_cache:
        cache_manager = CacheManager(ttl=config_manager.get('cache_ttl'))
    
    return Rapid7Client(api_key, region, cache_manager), cache_manager

def format_severity(severity):
    """Format severity with color coding"""
    if severity == 'CRITICAL':
        return f"[bold red]{severity}[/bold red]"
    elif severity == 'HIGH':
        return f"[red]{severity}[/red]"
    elif severity == 'MEDIUM':
        return f"[yellow]{severity}[/yellow]"
    elif severity == 'LOW':
        return f"[blue]{severity}[/blue]"
    elif severity in ['INFO', 'INFORMATIONAL']:
        return f"[dim]{severity}[/dim]"
    else:
        return f"[dim]{severity}[/dim]"

def extract_vulnerability_title(vuln):
    """Extract meaningful vulnerability title from variance data"""
    variances = vuln.get('variances', [])
    if variances:
        attack_info = variances[0].get('attack', {})
        attack_id = attack_info.get('id', '')
        return f'{attack_id} Vulnerability' if attack_id else 'Unknown Vulnerability'
    return 'Unknown Vulnerability'

def count_vulnerabilities_by_severity(vuln_data):
    """Count vulnerabilities grouped by severity"""
    severity_counts = {'CRITICAL': 0, 'HIGH': 0, 'MEDIUM': 0, 'LOW': 0, 'INFO': 0}
    for vuln in vuln_data:
        severity = vuln.get('severity', 'UNKNOWN')
        # Map INFORMATIONAL to INFO for consistency
        if severity == 'INFORMATIONAL':
            severity = 'INFO'
        if severity in severity_counts:
            severity_counts[severity] += 1
    return severity_counts

def format_duration(submit_time, completion_time, status):
    """Calculate and format duration between timestamps"""
    if submit_time == 'N/A':
        return 'N/A'
    
    try:
        start = datetime.fromisoformat(submit_time.replace('Z', '+00:00'))
        
        if completion_time != 'N/A':
            end = datetime.fromisoformat(completion_time.replace('Z', '+00:00'))
        elif status == 'RUNNING':
            end = datetime.now(start.tzinfo)
        else:
            return 'N/A'
        
        duration_delta = end - start
        total_seconds = int(duration_delta.total_seconds())
        
        # Format duration
        if total_seconds < 60:
            duration = f"{total_seconds}s"
        elif total_seconds < 3600:
            minutes = total_seconds // 60
            seconds = total_seconds % 60
            duration = f"{minutes}m {seconds}s"
        else:
            hours = total_seconds // 3600
            minutes = (total_seconds % 3600) // 60
            duration = f"{hours}h {minutes}m"
        
        # Add indicator for running scans
        if status == 'RUNNING':
            duration += "..."
            
        return duration
    except (ValueError, TypeError):
        return 'Running...' if status == 'RUNNING' else 'N/A'

def display_vulnerabilities_table(vuln_data, limit, total_vulns):
    """Display vulnerabilities in a formatted table"""
    if total_vulns == 0:
        console.print("[green]✅ No vulnerabilities found![/green]")
        return
    
    displayed_vulns = vuln_data[:limit]
    table = Table(title=f"Vulnerabilities (showing {len(displayed_vulns)} of {total_vulns})")
    table.add_column("Severity", style="white", width=8)
    table.add_column("Vulnerability", style="cyan", width=40)
    table.add_column("URL", style="blue", width=50)
    table.add_column("Status", style="green", width=12)
    
    for vuln in displayed_vulns:
        severity = vuln.get('severity', 'UNKNOWN')
        title = extract_vulnerability_title(vuln)
        url = vuln.get('root_cause', {}).get('url', 'N/A')
        status = vuln.get('status', 'N/A')
        
        # Truncate long strings
        if len(title) > 37:
            title = title[:34] + "..."
        if len(url) > 47:
            url = url[:44] + "..."
        
        table.add_row(format_severity(severity), title, url, status)
    
    console.print(table)
    
    if total_vulns > limit:
        console.print(f"\n[dim]💡 Use --limit {total_vulns} to see all vulnerabilities[/dim]")

def fetch_app_name(client, app_id):
    """Fetch application name from ID"""
    try:
        app = client.get_app(app_id)
        return app.get('name', 'Unknown')
    except:
        return f'App ID: {app_id}'


def _is_valid_uuid(value):
    """Check if value looks like a UUID"""
    import re
    uuid_pattern = re.compile(r'^[a-f0-9]{8}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{12}$', re.IGNORECASE)
    return bool(uuid_pattern.match(str(value)))


def _find_app_by_name(client, app_name):
    """Find app ID by exact name match (case-insensitive)"""
    try:
        apps = client.list_apps()
        app_name_lower = app_name.lower()
        
        # Only exact match
        for app in apps.get('data', []):
            if app.get('name', '').lower() == app_name_lower:
                return app.get('id')
                
        return None
    except Exception:
        return None


def _find_similar_app_names(client, app_name):
    """Find apps with similar names for suggestions"""
    try:
        apps = client.list_apps()
        app_name_lower = app_name.lower()
        similar = []
        
        for app in apps.get('data', []):
            name = app.get('name', '')
            name_lower = name.lower()
            # Find apps that contain the search term or vice versa
            if (app_name_lower in name_lower or 
                any(word in name_lower for word in app_name_lower.split()) or
                any(word in app_name_lower for word in name_lower.split())):
                similar.append(name)
        
        return similar[:5]  # Return up to 5 suggestions
    except Exception:
        return []

# ============================================================================
# Command Groups
# ============================================================================

@click.group('scan')
def scan_group():
    """Manage scans"""
    pass

@click.group('app')
def app_group():
    """Manage applications"""
    pass

@click.group('appsec')
def appsec_group():
    """web app scans, findings"""
    pass

# Add app and scan as subcommands of appsec
appsec_group.add_command(app_group)
appsec_group.add_command(scan_group)

# ============================================================================
# Application Commands
# ============================================================================

@app_group.command('list')
@click.option('--output', type=click.Choice(['simple', 'table', 'json']),
              help='Output format')
@click.pass_context
def list_apps(ctx, output):
    """List all applications with latest scan info"""
    try:
        client, cache_manager = get_client(ctx)
        apps = client.list_apps()
        
        config_manager = ConfigManager()
        use_format = determine_output_format(output, config_manager)
        
        # For table output, also fetch latest scan info
        app_scan_map = {}
        if use_format == 'table':
            with Progress(SpinnerColumn(), TextColumn("Fetching latest scans..."), TimeElapsedColumn()) as progress:
                progress.add_task("Fetching", total=None)
                all_scans = client.list_scans()
                
                # Group scans by app ID and find latest
                for scan in all_scans.get('data', []):
                    app_id = scan.get('app', {}).get('id')
                    if app_id:
                        submit_time = scan.get('submit_time', '')
                        if app_id not in app_scan_map or submit_time > app_scan_map[app_id]['submit_time']:
                            app_scan_map[app_id] = {
                                'scan_id': scan.get('id', 'N/A'),
                                'submit_time': submit_time,
                                'status': scan.get('status', 'N/A')
                            }
        
        # Output
        if use_format == 'json':
            click.echo(json.dumps(apps, indent=2))
        elif use_format == 'simple':
            for app in apps.get('data', []):
                click.echo(f"{app.get('name', 'N/A')} {app.get('id', 'N/A')}")
        else:  # table
            table = Table(title="Applications")
            table.add_column("ID", style="cyan", width=36)
            table.add_column("Name", style="green", width=25)
            table.add_column("Latest Scan ID", style="yellow", width=36)
            table.add_column("Status", style="magenta", width=12)
            
            for app in apps.get('data', []):
                app_id = app.get('id', 'N/A')
                scan_info = app_scan_map.get(app_id, {})
                
                table.add_row(
                    app_id,
                    app.get('name', 'N/A'),
                    scan_info.get('scan_id', 'No scans'),
                    scan_info.get('status', 'N/A')
                )
            console.print(table)
            
    except Exception as e:
        console.print(f"[red]Error: {str(e)}[/red]")
        raise click.Abort()

@app_group.command('get')
@click.argument('app_identifier')
@click.option('--limit', type=int, default=20, help='Maximum number of vulnerabilities to show')
@click.option('--output', type=click.Choice(['simple', 'table', 'json']),
              help='Output format')
@click.option('--no-cache', is_flag=True, help='Disable caching for this request')
@click.pass_context
def get_app_latest_scan(ctx, app_identifier, limit, output, no_cache):
    """Get latest successful scan results for an application (by ID or name)
    
    Examples:
      r7 appsec app get 'Domain Trades'
      r7 appsec app get a1b2c3d4-e5f6-7890-abcd-ef1234567890
    """
    try:
        client, cache_manager = get_client(ctx, no_cache)
        
        # Resolve app identifier to app_id
        app_id = app_identifier
        app_name_provided = None
        
        # If it doesn't look like a UUID, treat as name and try to resolve
        if not _is_valid_uuid(app_identifier):
            app_name_provided = app_identifier
            resolved_app_id = _find_app_by_name(client, app_identifier)
            if not resolved_app_id:
                # Try to show similar apps to help user
                console.print(f"[red]❌ Application not found: '{app_identifier}'[/red]")
                
                # Show similar matches first
                try:
                    similar_apps = _find_similar_app_names(client, app_identifier)
                    if similar_apps:
                        console.print("\n[yellow]Did you mean one of these?[/yellow]")
                        for app_name in similar_apps:
                            console.print(f"  • {app_name}")
                    else:
                        # Fallback to showing first few apps
                        console.print("\n[dim]Available applications:[/dim]")
                        apps = client.list_apps()
                        for app in apps.get('data', [])[:10]:  # Show first 10
                            console.print(f"  • {app.get('name', 'N/A')}")
                        if len(apps.get('data', [])) > 10:
                            console.print(f"  ... and {len(apps.get('data', [])) - 10} more")
                    console.print("\n[dim]Use 'r7 appsec app list' to see all applications[/dim]")
                except:
                    pass
                raise click.Abort()
            app_id = resolved_app_id
            console.print(f"[dim]Found app '{app_name_provided}' → {app_id}[/dim]\n")
        
        # Find latest successful scan for this app (paginate through all scans if needed)
        import logging
        # Temporarily suppress debug logs during pagination to clean up progress display
        logger = logging.getLogger()
        original_level = logger.level
        if original_level <= logging.DEBUG:
            logger.setLevel(logging.INFO)
        
        try:
            with Progress(SpinnerColumn(), TextColumn("Finding latest successful scan..."), TimeElapsedColumn()) as progress:
                task = progress.add_task("Searching", total=None)
                
                latest_successful_scan = None
                page_index = 0
                page_size = 50
                total_scans_checked = 0
                
                while latest_successful_scan is None:
                    progress.update(task, description=f"Searching page {page_index + 1} ({total_scans_checked} scans checked)...")
                    app_scans = client.list_scans(app_id, index=page_index, size=page_size)
                    scans_data = app_scans.get('data', [])
                    
                    # If no scans on this page, we've reached the end
                    if not scans_data:
                        break
                    
                    total_scans_checked += len(scans_data)
                    
                    # Look for first COMPLETE scan on this page
                    for scan in scans_data:
                        if scan.get('status') == 'COMPLETE':
                            latest_successful_scan = scan
                            break
                    
                    page_index += 1
                    
                    # Safety check - don't paginate forever
                    if page_index > 20:  # Max 1000 scans (20 * 50)
                        break
                
                if not latest_successful_scan:
                    console.print(f"[yellow]No successful scans found after checking {total_scans_checked} scans across {page_index} pages[/yellow]")
                    return
                
                scan_id = latest_successful_scan.get('id')
        finally:
            # Restore original log level
            logger.setLevel(original_level)
        
        # Print after progress context closes
        console.print(f"[dim]Using latest successful scan: {scan_id}[/dim]\n")
        
        # Get scan details and vulnerabilities
        with Progress(SpinnerColumn(), TextColumn("Fetching scan results..."), TimeElapsedColumn()) as progress:
            progress.add_task("Fetching", total=None)
            scan = client.get_scan(scan_id)
            vulnerabilities = client.get_scan_vulnerabilities(scan_id, size=limit)
            # Use provided name if available, otherwise fetch it
            app_name = app_name_provided or fetch_app_name(client, app_id)
        
        if output == 'json':
            result = {
                'app_id': app_id,
                'latest_successful_scan': scan,
                'vulnerabilities': vulnerabilities
            }
            click.echo(json.dumps(result, indent=2))
        else:
            # Display scan summary
            scan_data = scan if isinstance(scan, dict) else {}
            vuln_data = vulnerabilities.get('data', [])
            severity_counts = count_vulnerabilities_by_severity(vuln_data)
            total_vulns = sum(severity_counts.values())
            
            # Scan summary panel
            panel_content = f"""[bold cyan]Application:[/bold cyan] {app_name} ({app_id})
[bold cyan]Latest Successful Scan ID:[/bold cyan] {scan_data.get('id', 'N/A')}
[bold cyan]Status:[/bold cyan] {scan_data.get('status', 'N/A')}
[bold cyan]Started:[/bold cyan] {scan_data.get('submit_time', 'N/A')}
[bold cyan]Completed:[/bold cyan] {scan_data.get('completion_time', 'N/A')}
[bold cyan]Scan Type:[/bold cyan] {scan_data.get('scan_type', 'N/A')}

[bold yellow]Vulnerabilities Found:[/bold yellow] {total_vulns}
[bold red]Critical:[/bold red] {severity_counts['CRITICAL']}  [bold red]High:[/bold red] {severity_counts['HIGH']}  [bold yellow]Medium:[/bold yellow] {severity_counts['MEDIUM']}  [bold blue]Low:[/bold blue] {severity_counts['LOW']}  [bold dim]Info:[/bold dim] {severity_counts['INFO']}"""
            
            console.print(Panel(panel_content, title="Latest Successful Scan Results", expand=False))
            console.print()
            
            # Display vulnerabilities table
            display_vulnerabilities_table(vuln_data, limit, total_vulns)
            
    except Exception as e:
        console.print(f"[red]Error: {str(e)}[/red]")
        raise click.Abort()

# ============================================================================
# Scan Commands
# ============================================================================

@scan_group.command('list')
@click.argument('app_identifier', required=False)
@click.option('--output', type=click.Choice(['simple', 'table', 'json']),
              help='Output format')
@click.option('--limit', type=int, default=None, help='Maximum number of scans to return')
@click.option('--no-cache', is_flag=True, help='Disable caching for this request')
@click.pass_context
def list_scans(ctx, app_identifier, output, limit, no_cache):
    """List scans (optionally filtered by application ID or name)
    
    Examples:
      r7 appsec scan list                          # List all scans
      r7 appsec scan list 'Confluence'             # List scans for app named Confluence
      r7 appsec scan list a1b2c3d4-e5f6-7890-...   # List scans for app with this ID
    """
    try:
        client, cache_manager = get_client(ctx, no_cache)
        
        # Resolve app identifier to app_id if provided
        app_id = None
        app_name_provided = None
        
        if app_identifier:
            # If it doesn't look like a UUID, treat as name and try to resolve
            if not _is_valid_uuid(app_identifier):
                app_name_provided = app_identifier
                resolved_app_id = _find_app_by_name(client, app_identifier)
                if not resolved_app_id:
                    # Try to show similar apps to help user
                    console.print(f"[red]❌ Application not found: '{app_identifier}'[/red]")
                    
                    # Show similar matches first
                    try:
                        similar_apps = _find_similar_app_names(client, app_identifier)
                        if similar_apps:
                            console.print("\n[yellow]Did you mean one of these?[/yellow]")
                            for app_name in similar_apps:
                                console.print(f"  • {app_name}")
                        else:
                            # Fallback to showing first few apps
                            console.print("\n[dim]Available applications:[/dim]")
                            apps = client.list_apps()
                            for app in apps.get('data', [])[:10]:  # Show first 10
                                console.print(f"  • {app.get('name', 'N/A')}")
                            if len(apps.get('data', [])) > 10:
                                console.print(f"  ... and {len(apps.get('data', [])) - 10} more")
                        console.print("\n[dim]Use 'r7 appsec app list' to see all applications[/dim]")
                    except:
                        pass
                    raise click.Abort()
                app_id = resolved_app_id
                console.print(f"[dim]Found app '{app_name_provided}' → {app_id}[/dim]\n")
            else:
                app_id = app_identifier
        
        config_manager = ConfigManager()
        use_format = determine_output_format(output, config_manager)
        
        # Get applications for name lookup (if showing table format)
        app_names = {}
        if use_format == 'table':
            apps_cache_key = "apps_all"
            cached_apps = cache_manager.get('appsec_api', apps_cache_key) if cache_manager else None
            
            if cached_apps:
                apps_data = cached_apps
            else:
                apps_data = client.list_apps()
                if cache_manager:
                    cache_manager.set('appsec_api', apps_cache_key, apps_data)
            
            # Create app ID to name mapping
            for app in apps_data.get('data', []):
                app_names[app.get('id')] = app.get('name', 'Unknown')
        
        # Fetch scans with caching
        cache_key = "scans_all" if not app_id else f"scans_app_{app_id}"
        cached_result = None
        if cache_manager:
            cached_result = cache_manager.get('appsec_api', cache_key)
            if cached_result and output != 'json':
                console.print("📋 Using cached result", style="dim")
        
        if cached_result:
            scans = cached_result
        else:
            if output != 'json':
                with Progress(SpinnerColumn(), TextColumn("Fetching scans..."), TimeElapsedColumn()) as progress:
                    progress.add_task("Fetching", total=None)
                    scans = client.list_scans(app_id)
            else:
                scans = client.list_scans(app_id)
            if cache_manager:
                cache_manager.set('appsec_api', cache_key, scans)
        
        # Apply limit
        data = scans.get('data', [])[:limit] if limit else scans.get('data', [])
        
        if output == 'json':
            click.echo(json.dumps({'data': data}, indent=2))
        elif output == 'simple':
            for scan in data:
                console.print(f"{scan.get('id', 'N/A')}: {scan.get('status', 'N/A')}")
        else:  # table
            title = f"Scans for {app_name_provided or f'App {app_id}'}" if app_id else "All Scans"
            if limit:
                title += f" (limit {limit})"
                
            table = Table(title=title)
            table.add_column("ID", style="cyan", width=36)
            table.add_column("Application", style="magenta", width=25)
            table.add_column("Status", style="green", width=10)
            table.add_column("Started", style="blue", width=20)
            table.add_column("Completed", style="yellow", width=20)
            table.add_column("Duration", style="white", width=12)
            
            for scan in data:
                app_id_from_scan = scan.get('app', {}).get('id', 'N/A')
                app_name = app_names.get(app_id_from_scan, f"ID: {app_id_from_scan}" if app_id_from_scan != 'N/A' else 'Unknown')
                
                duration = format_duration(
                    scan.get('submit_time', 'N/A'),
                    scan.get('completion_time', 'N/A'),
                    scan.get('status', 'N/A')
                )
                
                table.add_row(
                    scan.get('id', 'N/A'),
                    app_name,
                    scan.get('status', 'N/A'),
                    scan.get('submit_time', 'N/A'),
                    scan.get('completion_time', 'N/A'),
                    duration
                )
            
            console.print(table)
            
    except Exception as e:
        console.print(f"[red]Error: {str(e)}[/red]")
        raise click.Abort()

@scan_group.command('get')
@click.argument('scan_id')
@click.option('--output', type=click.Choice(['simple', 'table', 'json']),
              help='Output format')
@click.option('--limit', type=int, default=20, help='Maximum number of vulnerabilities to show')
@click.option('--no-cache', is_flag=True, help='Disable caching for this request')
@click.pass_context
def get_scan(ctx, scan_id, output, limit, no_cache):
    """Get scan results including vulnerabilities found"""
    try:
        client, cache_manager = get_client(ctx, no_cache)
        
        # Get scan metadata and vulnerabilities
        if output != 'json':
            with Progress(SpinnerColumn(), TextColumn("Fetching scan results..."), TimeElapsedColumn()) as progress:
                progress.add_task("Fetching", total=None)
                scan = client.get_scan(scan_id)
                vulnerabilities = client.get_scan_vulnerabilities(scan_id, size=limit)
                
                # Get application name for display
                app_id = scan.get('app', {}).get('id')
                app_name = fetch_app_name(client, app_id) if app_id else 'Unknown'
        else:
            scan = client.get_scan(scan_id)
            vulnerabilities = client.get_scan_vulnerabilities(scan_id, size=limit)
            app_name = 'Unknown'
        
        if output == 'json':
            result = {
                'scan': scan,
                'vulnerabilities': vulnerabilities
            }
            click.echo(json.dumps(result, indent=2))
        else:
            # Display scan summary
            scan_data = scan if isinstance(scan, dict) else {}
            app_info = scan_data.get('app', {})
            vuln_data = vulnerabilities.get('data', [])
            severity_counts = count_vulnerabilities_by_severity(vuln_data)
            total_vulns = sum(severity_counts.values())
            
            # Scan summary panel
            panel_content = f"""[bold cyan]Scan ID:[/bold cyan] {scan_data.get('id', 'N/A')}
[bold cyan]Status:[/bold cyan] {scan_data.get('status', 'N/A')}
[bold cyan]Application:[/bold cyan] {app_name} ({app_info.get('id', 'N/A')})
[bold cyan]Started:[/bold cyan] {scan_data.get('submit_time', 'N/A')}
[bold cyan]Completed:[/bold cyan] {scan_data.get('completion_time', 'N/A')}
[bold cyan]Scan Type:[/bold cyan] {scan_data.get('scan_type', 'N/A')}

[bold yellow]Vulnerabilities Found:[/bold yellow] {total_vulns}
[bold red]Critical:[/bold red] {severity_counts['CRITICAL']}  [bold red]High:[/bold red] {severity_counts['HIGH']}  [bold yellow]Medium:[/bold yellow] {severity_counts['MEDIUM']}  [bold blue]Low:[/bold blue] {severity_counts['LOW']}  [bold dim]Info:[/bold dim] {severity_counts['INFO']}"""
            
            console.print(Panel(panel_content, title="Scan Results Summary", expand=False))
            console.print()
            
            # Display vulnerabilities table
            display_vulnerabilities_table(vuln_data, limit, total_vulns)
            
    except Exception as e:
        console.print(f"[red]Error: {str(e)}[/red]")
        raise click.Abort()