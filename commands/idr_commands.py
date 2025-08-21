import click
import json
import logging
from rich.console import Console
from rich.table import Table
from rich.panel import Panel
from utils.config import ConfigManager
from utils.credentials import CredentialManager
from utils.cache import CacheManager
from api.client import Rapid7Client
from utils.exceptions import *
from commands.logs_commands import siem_logs_group

logger = logging.getLogger(__name__)
console = Console()

def build_investigation_rrn(investigation_id, region, organization_id):
    """Build full investigation RRN from parts"""
    return f"rrn:investigation:{region}:{organization_id}:investigation:{investigation_id}"

def extract_and_save_org_id(config_manager, investigation_data):
    """Extract organization_id from investigation data and save to config if not already set"""
    if config_manager.get('organization_id'):
        return  # Already have org_id
        
    # Check if data is a list or single item
    investigations = investigation_data.get('data', []) if isinstance(investigation_data, dict) else [investigation_data]
    
    for investigation in investigations:
        rrn = investigation.get('rrn', '')
        if rrn and ':' in rrn:
            parts = rrn.split(':')
            if len(parts) >= 6:
                org_id = parts[3]
                config_manager.set('organization_id', org_id)
                config_manager.save_config()
                logger.debug(f"Saved organization_id: {org_id}")
                return

def resolve_investigation_id(client, investigation_id, region, config_manager, org_id_override=None):
    """Convert short investigation ID to full RRN if needed"""
    if investigation_id.startswith('rrn:investigation:'):
        return investigation_id
    
    # Use org_id from CLI override, config, or attempt to get from API
    org_id = org_id_override or config_manager.get('organization_id')
    
    if org_id:
        return build_investigation_rrn(investigation_id, region, org_id)
    
    # Fallback: get organization_id from a sample investigation and save it
    try:
        sample_investigations = client.list_investigations({'limit': 1})
        if sample_investigations.get('data') and len(sample_investigations['data']) > 0:
            sample_rrn = sample_investigations['data'][0].get('rrn', '')
            if sample_rrn:
                # Extract org_id from sample RRN
                parts = sample_rrn.split(':')
                if len(parts) >= 6:
                    org_id = parts[3]
                    # Store org_id for future use
                    config_manager.set('organization_id', org_id)
                    config_manager.save_config()
                    return build_investigation_rrn(investigation_id, region, org_id)
    except:
        pass
    
    return investigation_id  # Return original if we can't build it

@click.group('siem')
@click.pass_context
def siem_group(ctx):
    """search logs, manage alerts/investigations"""
    pass

# Add logs subcommand to SIEM
siem_group.add_command(siem_logs_group)

@siem_group.group('investigation')
@click.pass_context
def investigation_group(ctx):
    """Manage investigations"""
    pass

@investigation_group.command('list')
@click.option('--status', multiple=True, type=click.Choice(['OPEN', 'INVESTIGATING', 'CLOSED']),
              help='Filter by investigation status (can be used multiple times)')
@click.option('--priority', multiple=True, type=click.Choice(['LOW', 'MEDIUM', 'HIGH', 'CRITICAL']),
              help='Filter by investigation priority (can be used multiple times)')
@click.option('--assignee', help='Filter by assignee email address')
@click.option('--start-time', help='Start time filter (ISO 8601 format)')
@click.option('--end-time', help='End time filter (ISO 8601 format)')
@click.option('--output', type=click.Choice(['simple', 'table', 'json']), default='table',
              help='Output format')
@click.option('--limit', type=int, default=None, help='Maximum number of investigations to return')
@click.option('--no-cache', is_flag=True, help='Disable caching for this request')
@click.option('--full-output', is_flag=True, help='Include all fields in JSON output (default shows minimal fields matching table view)')
@click.pass_context
def list_investigations(ctx, status, priority, assignee, start_time, end_time, output, limit, no_cache, full_output):
    """List investigations with optional filtering"""
    try:
        config_manager = ConfigManager()
        config_manager.validate()

        # Credentials and region
        api_key = CredentialManager.get_api_key(ctx.obj.get('api_key'))
        if not api_key:
            raise AuthenticationError("API key not found. Use 'r7 config cred store' to save credentials.")
        region = ctx.obj.get('region') or config_manager.get('region', 'us')

        # Cache
        cache_manager = None
        if config_manager.get('cache_enabled') and not no_cache:
            cache_manager = CacheManager(ttl=config_manager.get('cache_ttl'))
        client = Rapid7Client(api_key, region, cache_manager)

        # Build query parameters
        params = {}
        if status:
            params['statuses'] = ','.join(status)
        if priority:
            params['priorities'] = ','.join(priority)
        if assignee:
            params['assignee.email'] = assignee
        if start_time:
            params['start_time'] = start_time
        if end_time:
            params['end_time'] = end_time
        if limit is not None:
            params['size'] = limit

        # Fetch investigations
        investigations = client.list_investigations(params)
        
        # Auto-save organization_id from response
        extract_and_save_org_id(config_manager, investigations)

        # Use data directly from API response
        data = investigations.get('data', []) or []
        investigations_display = {'data': data}

        # Output
        if output == 'json':
            click.echo(json.dumps(investigations_display, indent=2))
        elif output == 'simple':
            for investigation in data:
                # Extract short ID for simple output too
                investigation_id = investigation.get('rrn', 'N/A')
                if ':' in investigation_id:
                    parts = investigation_id.split(':')
                    if len(parts) >= 6:
                        investigation_id = parts[-1]
                click.echo(f"{investigation_id}: {investigation.get('title', 'N/A')} [{investigation.get('status', 'N/A')}]")
        else:  # table
            title = "Investigations"
            if limit is not None:
                title += f" (limit {limit})"
            table = Table(title=title)
            table.add_column("ID", style="cyan")
            table.add_column("Title", style="green")
            table.add_column("Status", style="yellow")
            table.add_column("Priority", style="red")
            table.add_column("Assignee", style="blue")
            table.add_column("Created", style="dim")

            for investigation in data:
                # Extract just the investigation ID from RRN
                # Format: rrn:investigation:region:org_id:investigation:INVESTIGATION_ID
                investigation_id = investigation.get('rrn', 'N/A')
                if ':' in investigation_id:
                    parts = investigation_id.split(':')
                    if len(parts) >= 6:  # Should have at least 6 parts
                        investigation_id = parts[-1]  # Last part is the actual investigation ID
                
                assignee_name = "Unassigned"
                if investigation.get('assignee'):
                    assignee_name = investigation['assignee'].get('name', investigation['assignee'].get('email', 'Unknown'))
                
                # Format created time to be more readable
                created_time = investigation.get('created_time', 'N/A')
                if created_time != 'N/A' and 'T' in created_time:
                    created_time = created_time.split('T')[0]  # Just show date part
                
                table.add_row(
                    investigation_id,
                    investigation.get('title', 'N/A')[:50] + ('...' if len(investigation.get('title', '')) > 50 else ''),
                    investigation.get('status', 'N/A'),
                    investigation.get('priority', 'N/A'),
                    assignee_name,
                    created_time
                )
            console.print(table)
    except Exception as e:
        console.print(f"[red]Error: {str(e)}[/red]")
        raise click.Abort()

@investigation_group.command('get')
@click.argument('investigation_id')
@click.option('--output', type=click.Choice(['simple', 'table', 'json']), default='table',
              help='Output format')
@click.pass_context
def get_investigation(ctx, investigation_id, output):
    """Get investigation details"""
    try:
        config_manager = ConfigManager()
        cache_manager = CacheManager()
        
        # Get credentials
        api_key = CredentialManager.get_api_key(ctx.obj.get('api_key'))
        if not api_key:
            raise AuthenticationError("API key not found. Use 'r7 config cred store' to save credentials.")
        
        # Get region and org_id
        region = ctx.obj.get('region') or config_manager.get('region', 'us')
        org_id = ctx.obj.get('org_id')
        
        # Create client and make request
        client = Rapid7Client(api_key, region, cache_manager)
        full_investigation_id = resolve_investigation_id(client, investigation_id, region, config_manager, org_id)
        investigation = client.get_investigation(full_investigation_id)
        
        # Auto-save organization_id from response
        extract_and_save_org_id(config_manager, investigation)
        
        if output == 'json':
            click.echo(json.dumps(investigation, indent=2))
        else:
            # get_investigation returns data directly
            investigation_data = investigation if isinstance(investigation, dict) else {}
            
            # Extract short ID from RRN for display
            display_id = investigation_data.get('rrn', 'N/A')
            if ':' in display_id:
                parts = display_id.split(':')
                if len(parts) >= 6:
                    display_id = parts[-1]
            
            assignee_info = "Unassigned"
            if investigation_data.get('assignee'):
                assignee = investigation_data['assignee']
                assignee_info = f"{assignee.get('name', assignee.get('email', 'Unknown'))}"
            
            panel_content = f"""
[bold cyan]ID:[/bold cyan] {display_id}
[bold cyan]Title:[/bold cyan] {investigation_data.get('title', 'N/A')}
[bold cyan]Status:[/bold cyan] {investigation_data.get('status', 'N/A')}
[bold cyan]Priority:[/bold cyan] {investigation_data.get('priority', 'N/A')}
[bold cyan]Disposition:[/bold cyan] {investigation_data.get('disposition', 'N/A')}
[bold cyan]Assignee:[/bold cyan] {assignee_info}
[bold cyan]Created:[/bold cyan] {investigation_data.get('created_time', 'N/A')}
[bold cyan]Updated:[/bold cyan] {investigation_data.get('last_accessed', 'N/A')}
            """
            console.print(Panel(panel_content, title=f"Investigation: {investigation_data.get('title', 'N/A')}", expand=False))
            
    except Exception as e:
        console.print(f"[red]Error: {str(e)}[/red]")
        raise click.Abort()

@investigation_group.command('create')
@click.argument('title')
@click.option('--priority', type=click.Choice(['LOW', 'MEDIUM', 'HIGH', 'CRITICAL']), default='MEDIUM',
              help='Investigation priority')
@click.option('--status', type=click.Choice(['OPEN', 'INVESTIGATING', 'CLOSED']), default='OPEN',
              help='Investigation status')
@click.option('--disposition', type=click.Choice(['BENIGN', 'MALICIOUS', 'NOT_APPLICABLE', 'UNDECIDED']),
              help='Investigation disposition')
@click.option('--assignee', help='Assignee email address')
@click.option('--output', type=click.Choice(['simple', 'table', 'json']), default='table',
              help='Output format')
@click.pass_context
def create_investigation(ctx, title, priority, status, disposition, assignee, output):
    """Create a new investigation"""
    try:
        config_manager = ConfigManager()
        cache_manager = CacheManager()
        
        # Get credentials
        api_key = CredentialManager.get_api_key(ctx.obj.get('api_key'))
        if not api_key:
            raise AuthenticationError("API key not found. Use 'r7 config cred store' to save credentials.")
        
        # Get region
        region = ctx.obj.get('region') or config_manager.get('region', 'us')
        
        # Create client and make request
        client = Rapid7Client(api_key, region, cache_manager)
        
        # Build investigation data
        investigation_data = {
            'title': title,
            'priority': priority,
            'status': status
        }
        
        if disposition:
            investigation_data['disposition'] = disposition
        if assignee:
            investigation_data['assignee'] = {'email': assignee}
        
        investigation = client.create_investigation(investigation_data)
        
        if output == 'json':
            click.echo(json.dumps(investigation, indent=2))
        else:
            investigation_data = investigation if isinstance(investigation, dict) else {}
            console.print(f"[green]✓ Investigation created successfully[/green]")
            console.print(f"[bold cyan]ID:[/bold cyan] {investigation_data.get('id', 'N/A')}")
            console.print(f"[bold cyan]Title:[/bold cyan] {investigation_data.get('title', 'N/A')}")
            console.print(f"[bold cyan]Status:[/bold cyan] {investigation_data.get('status', 'N/A')}")
            
    except Exception as e:
        console.print(f"[red]Error: {str(e)}[/red]")
        raise click.Abort()

@investigation_group.command('set-status')
@click.argument('investigation_id')
@click.argument('status', type=click.Choice(['OPEN', 'INVESTIGATING', 'CLOSED']))
@click.option('--output', type=click.Choice(['simple', 'table', 'json']), default='simple',
              help='Output format')
@click.pass_context
def set_investigation_status(ctx, investigation_id, status, output):
    """Set investigation status"""
    try:
        config_manager = ConfigManager()
        cache_manager = CacheManager()
        
        # Get credentials
        api_key = CredentialManager.get_api_key(ctx.obj.get('api_key'))
        if not api_key:
            raise AuthenticationError("API key not found. Use 'r7 config cred store' to save credentials.")
        
        # Get region and org_id
        region = ctx.obj.get('region') or config_manager.get('region', 'us')
        org_id = ctx.obj.get('org_id')
        
        # Create client and make request
        client = Rapid7Client(api_key, region, cache_manager)
        full_investigation_id = resolve_investigation_id(client, investigation_id, region, config_manager, org_id)
        result = client.set_investigation_status(full_investigation_id, status)
        
        if output == 'json':
            click.echo(json.dumps(result, indent=2))
        else:
            console.print(f"[green]✓ Investigation {investigation_id} status set to {status}[/green]")
            
    except Exception as e:
        console.print(f"[red]Error: {str(e)}[/red]")
        raise click.Abort()

@investigation_group.command('set-priority')
@click.argument('investigation_id')
@click.argument('priority', type=click.Choice(['LOW', 'MEDIUM', 'HIGH', 'CRITICAL']))
@click.option('--output', type=click.Choice(['simple', 'table', 'json']), default='simple',
              help='Output format')
@click.pass_context
def set_investigation_priority(ctx, investigation_id, priority, output):
    """Set investigation priority"""
    try:
        config_manager = ConfigManager()
        cache_manager = CacheManager()
        
        # Get credentials
        api_key = CredentialManager.get_api_key(ctx.obj.get('api_key'))
        if not api_key:
            raise AuthenticationError("API key not found. Use 'r7 config cred store' to save credentials.")
        
        # Get region and org_id
        region = ctx.obj.get('region') or config_manager.get('region', 'us')
        org_id = ctx.obj.get('org_id')
        
        # Create client and make request
        client = Rapid7Client(api_key, region, cache_manager)
        full_investigation_id = resolve_investigation_id(client, investigation_id, region, config_manager, org_id)
        result = client.set_investigation_priority(full_investigation_id, priority)
        
        if output == 'json':
            click.echo(json.dumps(result, indent=2))
        else:
            console.print(f"[green]✓ Investigation {investigation_id} priority set to {priority}[/green]")
            
    except Exception as e:
        console.print(f"[red]Error: {str(e)}[/red]")
        raise click.Abort()

@investigation_group.command('assign')
@click.argument('investigation_id')
@click.argument('assignee_email')
@click.option('--output', type=click.Choice(['simple', 'table', 'json']), default='simple',
              help='Output format')
@click.pass_context
def assign_investigation(ctx, investigation_id, assignee_email, output):
    """Assign investigation to a user"""
    try:
        config_manager = ConfigManager()
        cache_manager = CacheManager()
        
        # Get credentials
        api_key = CredentialManager.get_api_key(ctx.obj.get('api_key'))
        if not api_key:
            raise AuthenticationError("API key not found. Use 'r7 config cred store' to save credentials.")
        
        # Get region and org_id
        region = ctx.obj.get('region') or config_manager.get('region', 'us')
        org_id = ctx.obj.get('org_id')
        
        # Create client and make request
        client = Rapid7Client(api_key, region, cache_manager)
        full_investigation_id = resolve_investigation_id(client, investigation_id, region, config_manager, org_id)
        result = client.assign_investigation(full_investigation_id, assignee_email)
        
        if output == 'json':
            click.echo(json.dumps(result, indent=2))
        else:
            console.print(f"[green]✓ Investigation {investigation_id} assigned to {assignee_email}[/green]")
            
    except Exception as e:
        console.print(f"[red]Error: {str(e)}[/red]")
        raise click.Abort()

@investigation_group.command('update')
@click.argument('investigation_id')
@click.option('--title', help='Update investigation title')
@click.option('--status', type=click.Choice(['OPEN', 'INVESTIGATING', 'CLOSED']),
              help='Update investigation status')
@click.option('--priority', type=click.Choice(['UNSPECIFIED', 'LOW', 'MEDIUM', 'HIGH', 'CRITICAL']),
              help='Update investigation priority')
@click.option('--disposition', type=click.Choice(['BENIGN', 'MALICIOUS', 'NOT_APPLICABLE']),
              help='Update investigation disposition')
@click.option('--assignee-email', help='Email address of user to assign the investigation to')
@click.option('--multi-customer', is_flag=True, help='Indicates multi-customer access (requires RRN format)')
@click.option('--output', type=click.Choice(['simple', 'table', 'json']), default='simple',
              help='Output format')
@click.pass_context
def update_investigation(ctx, investigation_id, title, status, priority, disposition, assignee_email, multi_customer, output):
    """Update multiple fields in a single operation for an investigation"""
    try:
        config_manager = ConfigManager()
        cache_manager = CacheManager()
        
        # Get credentials
        api_key = CredentialManager.get_api_key(ctx.obj.get('api_key'))
        if not api_key:
            raise AuthenticationError("API key not found. Use 'r7 config cred store' to save credentials.")
        
        # Get region and org_id
        region = ctx.obj.get('region') or config_manager.get('region', 'us')
        org_id = ctx.obj.get('org_id')
        
        # Create client
        client = Rapid7Client(api_key, region, cache_manager)
        
        # Resolve investigation ID to RRN if needed
        if multi_customer:
            # For multi-customer, must use RRN format
            if not investigation_id.startswith('rrn:investigation:'):
                raise APIError("Multi-customer access requires investigation ID in RRN format")
            full_investigation_id = investigation_id
        else:
            full_investigation_id = resolve_investigation_id(client, investigation_id, region, config_manager, org_id)
        
        # Build update data
        update_data = {}
        
        if title:
            update_data['title'] = title
        
        if status:
            update_data['status'] = status
        
        if priority:
            update_data['priority'] = priority
        
        if disposition:
            update_data['disposition'] = disposition
        
        if assignee_email:
            update_data['assignee'] = {'email': assignee_email}

        # Validate that at least one field is being updated
        if not update_data:
            console.print("[red]Error: At least one field must be specified for update[/red]")
            console.print("[yellow]Available options: --title, --status, --priority, --disposition, --assignee-email[/yellow]")
            raise click.Abort()

        # Update investigation
        result = client.update_investigation(full_investigation_id, update_data, multi_customer)
        
        if output == 'json':
            click.echo(json.dumps(result, indent=2))
        else:
            # Extract short ID for display
            display_id = investigation_id
            if ':' in display_id:
                parts = display_id.split(':')
                if len(parts) >= 6:
                    display_id = parts[-1]
            
            console.print(f"[green]✓ Investigation {display_id} updated successfully[/green]")
            
            # Show what was updated
            if title:
                console.print(f"  Title: {title}")
            if status:
                console.print(f"  Status: {status}")
            if priority:
                console.print(f"  Priority: {priority}")
            if disposition:
                console.print(f"  Disposition: {disposition}")
            if assignee_email:
                console.print(f"  Assigned to: {assignee_email}")
            
    except Exception as e:
        console.print(f"[red]Error: {str(e)}[/red]")
        raise click.Abort()

@investigation_group.command('alerts')
@click.argument('investigation_id')
@click.option('--limit', type=int, default=20, help='Maximum number of alerts to return (default: 20)')
@click.option('--output', type=click.Choice(['simple', 'table', 'json']), default='table',
              help='Output format')
@click.option('--no-cache', is_flag=True, help='Disable caching for this request')
@click.pass_context
def list_investigation_alerts(ctx, investigation_id, limit, output, no_cache):
    """List alerts associated with an investigation"""
    try:
        config_manager = ConfigManager()
        config_manager.validate()

        # Credentials and region
        api_key = CredentialManager.get_api_key(ctx.obj.get('api_key'))
        if not api_key:
            raise AuthenticationError("API key not found. Use 'r7 config cred store' to save credentials.")
        region = ctx.obj.get('region') or config_manager.get('region', 'us')
        org_id = ctx.obj.get('org_id')

        # Cache
        cache_manager = None
        if config_manager.get('cache_enabled') and not no_cache:
            cache_manager = CacheManager(ttl=config_manager.get('cache_ttl'))
        client = Rapid7Client(api_key, region, cache_manager)

        # Resolve investigation ID to full RRN if needed (same pattern as other investigation commands)
        full_investigation_id = resolve_investigation_id(client, investigation_id, region, config_manager, org_id)
        
        # Fetch investigation alerts
        alerts = client.list_investigation_alerts(full_investigation_id, size=limit)

        # Output
        if output == 'json':
            click.echo(json.dumps(alerts, indent=2))
        elif output == 'simple':
            data = alerts.get('data', []) or []
            for alert in data:
                # Investigation alerts API uses 'id' field instead of 'rrn'
                alert_id = alert.get('id', 'N/A')
                if ':' in alert_id:
                    parts = alert_id.split(':')
                    if len(parts) >= 6:
                        alert_id = parts[-1]
                title = alert.get('title', 'N/A')
                alert_type = alert.get('alert_type', 'N/A')
                click.echo(f"{alert_id}: {title} [Type: {alert_type}]")
        else:  # table
            table = Table(title=f"Investigation Alerts (Investigation: {investigation_id})")
            table.add_column("Alert ID", style="cyan")
            table.add_column("Title", style="green")
            table.add_column("Type", style="yellow")
            table.add_column("Source", style="blue")
            table.add_column("Created", style="dim")

            data = alerts.get('data', []) or []
            for alert in data:
                # Extract short ID from RRN - investigation alerts API uses 'id' field
                alert_id = alert.get('id', 'N/A')
                if ':' in alert_id:
                    parts = alert_id.split(':')
                    if len(parts) >= 6:
                        alert_id = parts[-1]
                
                title = alert.get('title', 'N/A')
                if len(title) > 50:
                    title = title[:47] + '...'
                
                # Format created time - investigation alerts API uses 'created_time'
                created_time = alert.get('created_time', 'N/A')
                if created_time != 'N/A' and 'T' in created_time:
                    created_time = created_time.split('T')[0]
                
                alert_type = alert.get('alert_type', 'N/A')
                if len(alert_type) > 25:
                    alert_type = alert_type[:22] + '...'
                
                alert_source = alert.get('alert_source', 'N/A')
                if len(alert_source) > 15:
                    alert_source = alert_source[:12] + '...'
                
                table.add_row(
                    alert_id,
                    title,
                    alert_type,
                    alert_source,
                    created_time
                )
            console.print(table)
            
            # Show helpful hint about using alert get
            if data:
                console.print(f"[dim]💡 Use 'r7 siem alert get <alert_id>' to view detailed information for any alert[/dim]")

    except Exception as e:
        console.print(f"[red]Error: {str(e)}[/red]")
        raise click.Abort()

# Alert Commands  
@siem_group.group('alert')
@click.pass_context
def alert_group(ctx):
    """Manage alerts"""
    pass

@alert_group.command('list')
@click.option('--limit', type=int, default=20, help='Maximum number of alerts to return (default: 20)')
@click.option('--rrns-only', is_flag=True, help='Return only alert RRNs without details')
@click.option('--output', type=click.Choice(['simple', 'table', 'json']), default='table',
              help='Output format')
@click.option('--no-cache', is_flag=True, help='Disable caching for this request')
@click.option('--full-output', is_flag=True, help='Include all fields in JSON output (default shows minimal fields matching table view)')
@click.pass_context
def list_alerts(ctx, limit, rrns_only, output, no_cache, full_output):
    """List alerts"""
    try:
        config_manager = ConfigManager()
        config_manager.validate()

        # Credentials and region
        api_key = CredentialManager.get_api_key(ctx.obj.get('api_key'))
        if not api_key:
            raise AuthenticationError("API key not found. Use 'r7 config cred store' to save credentials.")
        region = ctx.obj.get('region') or config_manager.get('region', 'us')

        # Cache
        cache_manager = None
        if config_manager.get('cache_enabled') and not no_cache:
            cache_manager = CacheManager(ttl=config_manager.get('cache_ttl'))
        client = Rapid7Client(api_key, region, cache_manager)

        # Search alerts with default parameters
        alerts = client.search_alerts(
            rrns_only=rrns_only,
            size=limit
        )

        # Output
        if output == 'json':
            if full_output:
                # Full output - include all fields
                click.echo(json.dumps(alerts, indent=2))
            else:
                # Minimal output - only include fields shown in table view
                if rrns_only:
                    # For rrns_only, keep simple structure
                    minimal_data = {
                        'rrns': alerts.get('rrns', []),
                        'metadata': alerts.get('metadata', {}),
                        'region_failures': alerts.get('region_failures', [])
                    }
                else:
                    # Create minimal alert data matching table view
                    minimal_alerts = []
                    for alert in alerts.get('alerts', []):
                        # Extract only the fields shown in the table
                        minimal_alert = {
                            'rrn': alert.get('rrn'),
                            'title': alert.get('title'),
                            'status': alert.get('status'),
                            'priority': alert.get('priority'),
                            'created_at': alert.get('created_at')
                        }
                        minimal_alerts.append(minimal_alert)
                    
                    minimal_data = {
                        'alerts': minimal_alerts,
                        'metadata': alerts.get('metadata', {}),
                        'region_failures': alerts.get('region_failures', [])
                    }
                
                click.echo(json.dumps(minimal_data, indent=2))
        elif output == 'simple':
            if rrns_only:
                # When rrns_only=True, the response contains RRNs in the 'rrns' field
                rrns = alerts.get('rrns', []) or []
                for rrn in rrns:
                    if ':' in rrn:
                        parts = rrn.split(':')
                        if len(parts) >= 6:
                            alert_id = parts[-1]
                            click.echo(alert_id)
                    else:
                        click.echo(rrn)
            else:
                data = alerts.get('alerts', []) or []
                for alert in data:
                    alert_id = alert.get('rrn', 'N/A')
                    if ':' in alert_id:
                        parts = alert_id.split(':')
                        if len(parts) >= 6:
                            alert_id = parts[-1]
                    title = alert.get('title', 'N/A')
                    status = alert.get('status', 'N/A')
                    click.echo(f"{alert_id}: {title} [{status}]")
        else:  # table
            if rrns_only:
                table = Table(title=f"Alert RRNs (limit {limit})")
                table.add_column("Alert ID", style="cyan")
                table.add_column("Full RRN", style="dim")

                rrns = alerts.get('rrns', []) or []
                for rrn in rrns:
                    if ':' in rrn:
                        parts = rrn.split(':')
                        if len(parts) >= 6:
                            alert_id = parts[-1]
                            table.add_row(alert_id, rrn)
                    else:
                        table.add_row(rrn, rrn)
                console.print(table)
            else:
                table = Table(title=f"Alerts (limit {limit})")
                table.add_column("ID", style="cyan")
                table.add_column("Title", style="green")
                table.add_column("Status", style="yellow")
                table.add_column("Priority", style="red")
                table.add_column("Created", style="dim")

                data = alerts.get('alerts', []) or []
                for alert in data:
                    # Extract short ID from RRN
                    alert_id = alert.get('rrn', 'N/A')
                    if ':' in alert_id:
                        parts = alert_id.split(':')
                        if len(parts) >= 6:
                            alert_id = parts[-1]
                    
                    title = alert.get('title', 'N/A')
                    if len(title) > 50:
                        title = title[:47] + '...'
                    
                    # Format created time  
                    created_time = alert.get('created_at', 'N/A')
                    if created_time != 'N/A' and 'T' in created_time:
                        created_time = created_time.split('T')[0]
                    
                    table.add_row(
                        alert_id,
                        title,
                        alert.get('status', 'N/A'),
                        alert.get('priority', 'N/A'),
                        created_time
                    )
                console.print(table)
    except Exception as e:
        console.print(f"[red]Error: {str(e)}[/red]")
        raise click.Abort()

@alert_group.command('get')
@click.argument('alert_id')
@click.option('--output', type=click.Choice(['simple', 'table', 'json']), default='table',
              help='Output format')
@click.pass_context
def get_alert(ctx, alert_id, output):
    """Get alert details by ID or RRN"""
    try:
        config_manager = ConfigManager()
        config_manager.validate()

        # Credentials and region
        api_key = CredentialManager.get_api_key(ctx.obj.get('api_key'))
        if not api_key:
            raise AuthenticationError("API key not found. Use 'r7 config cred store' to save credentials.")
        region = ctx.obj.get('region') or config_manager.get('region', 'us')

        # Cache
        cache_manager = None
        if config_manager.get('cache_enabled'):
            cache_manager = CacheManager(ttl=config_manager.get('cache_ttl'))
        client = Rapid7Client(api_key, region, cache_manager)

        # Build full RRN if only short ID provided
        if ':' not in alert_id:
            # Need to build the full RRN from short ID
            # Format: rrn:alerts:{region}:{org_id}:alert:1:{alert_id}
            org_id = config_manager.get('organization_id')
            if not org_id:
                # Try to get org_id from a sample alert list
                try:
                    sample_alerts = client.search_alerts(size=1)
                    if sample_alerts.get('alerts') and len(sample_alerts['alerts']) > 0:
                        sample_rrn = sample_alerts['alerts'][0].get('rrn', '')
                        if sample_rrn:
                            parts = sample_rrn.split(':')
                            if len(parts) >= 6:
                                org_id = parts[2]
                                config_manager.set('organization_id', org_id)
                                config_manager.save_config()
                except:
                    pass
            
            if org_id:
                alert_rrn = f"rrn:alerts:{region}:{org_id}:alert:1:{alert_id}"
            else:
                raise APIError("Cannot build alert RRN. Organization ID not found. Use full RRN instead.")
        else:
            alert_rrn = alert_id

        # Fetch alert details
        alert = client.get_alert(alert_rrn)

        # Output
        if output == 'json':
            click.echo(json.dumps(alert, indent=2))
        else:
            # Extract short ID for display
            display_id = alert.get('rrn', 'N/A')
            if ':' in display_id:
                parts = display_id.split(':')
                if len(parts) >= 6:
                    display_id = parts[-1]
            
            if output == 'simple':
                click.echo(f"ID: {display_id}")
                click.echo(f"Title: {alert.get('title', 'N/A')}")
                click.echo(f"Status: {alert.get('status', 'N/A')}")
                click.echo(f"Priority: {alert.get('priority', 'N/A')}")
                click.echo(f"Type: {alert.get('type', 'N/A')}")
                click.echo(f"Created: {alert.get('created_at', 'N/A')}")
                click.echo(f"Updated: {alert.get('updated_at', 'N/A')}")
                if alert.get('investigation_rrn'):
                    inv_parts = alert['investigation_rrn'].split(':')
                    inv_id = inv_parts[-1] if len(inv_parts) >= 6 else alert['investigation_rrn']
                    click.echo(f"Investigation: {inv_id}")
            else:  # table
                panel_content = f"""
[bold cyan]ID:[/bold cyan] {display_id}
[bold cyan]Title:[/bold cyan] {alert.get('title', 'N/A')}
[bold cyan]Status:[/bold cyan] {alert.get('status', 'N/A')}
[bold cyan]Priority:[/bold cyan] {alert.get('priority', 'N/A')}
[bold cyan]Type:[/bold cyan] {alert.get('type', 'N/A')}
[bold cyan]Disposition:[/bold cyan] {alert.get('disposition', 'N/A')}
[bold cyan]External Source:[/bold cyan] {alert.get('external_source', 'N/A')}
[bold cyan]Created:[/bold cyan] {alert.get('created_at', 'N/A')}
[bold cyan]Updated:[/bold cyan] {alert.get('updated_at', 'N/A')}
[bold cyan]Alerted At:[/bold cyan] {alert.get('alerted_at', 'N/A')}"""
                
                if alert.get('investigation_rrn'):
                    inv_parts = alert['investigation_rrn'].split(':')
                    inv_id = inv_parts[-1] if len(inv_parts) >= 6 else alert['investigation_rrn']
                    panel_content += f"\n[bold cyan]Investigation:[/bold cyan] {inv_id}"
                
                if alert.get('assignee'):
                    assignee = alert['assignee']
                    assignee_info = assignee.get('name', assignee.get('email', 'Unknown'))
                    panel_content += f"\n[bold cyan]Assignee:[/bold cyan] {assignee_info}"
                
                console.print(Panel(panel_content, title=f"Alert: {alert.get('title', 'N/A')}", expand=False))
                
                # Show rule keys of interest if available
                keys_of_interest = alert.get('rule_keys_of_interest', [])
                if keys_of_interest:
                    table = Table(title="Rule Keys of Interest")
                    table.add_column("Key", style="cyan")
                    table.add_column("Values", style="white")
                    
                    for key_info in keys_of_interest:
                        key_name = key_info.get('key', 'Unknown')
                        values = key_info.get('values', [])
                        values_str = ', '.join(str(v) for v in values[:3])  # Show first 3 values
                        if len(values) > 3:
                            values_str += f" (and {len(values) - 3} more)"
                        table.add_row(key_name, values_str)
                    
                    console.print(table)

    except Exception as e:
        console.print(f"[red]Error: {str(e)}[/red]")
        raise click.Abort()

@alert_group.command('update')
@click.argument('alert_id')
@click.option('--status', type=click.Choice(['UNMAPPED', 'OPEN', 'INVESTIGATING', 'WAITING', 'CLOSED']),
              help='Update alert status')
@click.option('--disposition', type=click.Choice(['UNMAPPED', 'BENIGN', 'SECURITY_TEST', 'MALICIOUS', 'FALSE_POSITIVE', 'UNKNOWN', 'NOT_APPLICABLE', 'UNDECIDED']),
              help='Update alert disposition')
@click.option('--priority', type=click.Choice(['UNMAPPED', 'INFO', 'LOW', 'MEDIUM', 'HIGH', 'CRITICAL']),
              help='Update alert priority')
@click.option('--assignee-id', help='User ID to assign the alert to')
@click.option('--investigation-rrn', help='RRN of investigation to associate alert with')
@click.option('--add-tags', help='Comma-separated list of tags to add')
@click.option('--remove-tags', help='Comma-separated list of tags to remove')
@click.option('--comment', help='Reason for updating the alert (for audit log)')
@click.option('--output', type=click.Choice(['simple', 'table', 'json']), default='simple',
              help='Output format')
@click.pass_context
def update_alert(ctx, alert_id, status, disposition, priority, assignee_id, investigation_rrn, 
                add_tags, remove_tags, comment, output):
    """Update a single alert"""
    try:
        config_manager = ConfigManager()
        config_manager.validate()

        # Credentials and region
        api_key = CredentialManager.get_api_key(ctx.obj.get('api_key'))
        if not api_key:
            raise AuthenticationError("API key not found. Use 'r7 config cred store' to save credentials.")
        region = ctx.obj.get('region') or config_manager.get('region', 'us')

        # Cache
        cache_manager = None
        if config_manager.get('cache_enabled'):
            cache_manager = CacheManager(ttl=config_manager.get('cache_ttl'))
        client = Rapid7Client(api_key, region, cache_manager)

        # Build full RRN if only short ID provided
        if ':' not in alert_id:
            org_id = config_manager.get('organization_id')
            if not org_id:
                # Try to get org_id from a sample alert list
                try:
                    sample_alerts = client.search_alerts(size=1)
                    if sample_alerts.get('alerts') and len(sample_alerts['alerts']) > 0:
                        sample_rrn = sample_alerts['alerts'][0].get('rrn', '')
                        if sample_rrn:
                            parts = sample_rrn.split(':')
                            if len(parts) >= 6:
                                org_id = parts[2]
                                config_manager.set('organization_id', org_id)
                                config_manager.save_config()
                except:
                    pass
            
            if org_id:
                alert_rrn = f"rrn:alerts:{region}:{org_id}:alert:1:{alert_id}"
            else:
                raise APIError("Cannot build alert RRN. Organization ID not found. Use full RRN instead.")
        else:
            alert_rrn = alert_id

        # Build update data
        update_data = {}
        
        if status:
            update_data['status'] = {'value': status}
        
        if disposition:
            update_data['disposition'] = {'value': disposition}
        
        if priority:
            update_data['priority'] = {'value': priority}
        
        if assignee_id:
            update_data['assignee_id'] = {'value': assignee_id}
        
        if investigation_rrn:
            update_data['investigation_rrn'] = {'value': investigation_rrn}
        
        if add_tags:
            tags_list = [tag.strip() for tag in add_tags.split(',')]
            update_data['tags'] = {'value': tags_list, 'action': 'ADD'}
        
        if remove_tags:
            if 'tags' in update_data:
                console.print("[yellow]Warning: Cannot add and remove tags in same request. Only remove operation will be applied.[/yellow]")
            tags_list = [tag.strip() for tag in remove_tags.split(',')]
            update_data['tags'] = {'value': tags_list, 'action': 'REMOVE'}
        
        if comment:
            update_data['comment'] = comment

        # Validate that at least one field is being updated
        if not update_data:
            console.print("[red]Error: At least one field must be specified for update[/red]")
            console.print("[yellow]Available options: --status, --disposition, --priority, --assignee-id, --investigation-rrn, --add-tags, --remove-tags, --comment[/yellow]")
            raise click.Abort()

        # Update alert
        result = client.update_alert(alert_rrn, update_data)

        # Output
        if output == 'json':
            click.echo(json.dumps(result, indent=2))
        else:
            # Extract short ID for display
            display_id = alert_rrn
            if ':' in display_id:
                parts = display_id.split(':')
                if len(parts) >= 6:
                    display_id = parts[-1]
            
            console.print(f"[green]✓ Alert {display_id} updated successfully[/green]")
            
            # Show what was updated
            if status:
                console.print(f"  Status: {status}")
            if disposition:
                console.print(f"  Disposition: {disposition}")
            if priority:
                console.print(f"  Priority: {priority}")
            if assignee_id:
                console.print(f"  Assigned to: {assignee_id}")
            if investigation_rrn:
                console.print(f"  Investigation: {investigation_rrn}")
            if add_tags:
                console.print(f"  Added tags: {add_tags}")
            if remove_tags:
                console.print(f"  Removed tags: {remove_tags}")

    except APIError as e:
        # Handle API errors with better messaging
        if hasattr(e, 'error_data') and e.error_data:
            error_data = e.error_data
            message = error_data.get('message', str(e))
            
            console.print(f"[red]Error: {message}[/red]")
            
            # Provide helpful context for common errors
            if "already belongs to an investigation" in message.lower():
                console.print("[yellow]💡 This alert is already part of an investigation.[/yellow]")
                console.print("[yellow]   Only tag changes are allowed for alerts in investigations.[/yellow]")
                console.print("[yellow]   To modify other fields, try:[/yellow]")
                console.print(f"[dim]   • r7 siem alert update {alert_id} --add-tags \"your-tag\"[/dim]")
                console.print(f"[dim]   • r7 siem alert get {alert_id} (to see current investigation)[/dim]")
                
                # Extract investigation info if available
                try:
                    alert_info = client.get_alert(alert_rrn)
                    if alert_info.get('investigation_rrn'):
                        inv_parts = alert_info['investigation_rrn'].split(':')
                        inv_id = inv_parts[-1] if len(inv_parts) >= 6 else alert_info['investigation_rrn']
                        console.print(f"[dim]   • r7 siem investigation update {inv_id} --status CLOSED[/dim]")
                except:
                    pass
            
            # Show validation errors if present
            validations = error_data.get('validations', [])
            if validations:
                console.print("[yellow]Validation errors:[/yellow]")
                for validation in validations:
                    console.print(f"[dim]  • {validation}[/dim]")
        else:
            console.print(f"[red]Error: {str(e)}[/red]")
        raise click.Abort()
    except Exception as e:
        console.print(f"[red]Error: {str(e)}[/red]")
        raise click.Abort()

# Comment Commands
@investigation_group.group('comment')
@click.pass_context
def comment_group(ctx):
    """Manage investigation comments"""
    pass

@comment_group.command('list')
@click.option('--target', help='Filter comments by target (investigation RRN)')
@click.option('--investigation-id', help='Filter comments by investigation ID (will be converted to RRN)')
@click.option('--output', type=click.Choice(['simple', 'table', 'json']), default='table',
              help='Output format')
@click.option('--limit', type=int, help='Maximum number of comments to return')
@click.pass_context
def list_comments(ctx, target, investigation_id, output, limit):
    """List comments with optional filtering"""
    try:
        config_manager = ConfigManager()
        cache_manager = CacheManager()
        
        # Get credentials
        api_key = CredentialManager.get_api_key(ctx.obj.get('api_key'))
        if not api_key:
            raise AuthenticationError("API key not found. Use 'r7 config cred store' to save credentials.")
        
        # Get region and org_id
        region = ctx.obj.get('region') or config_manager.get('region', 'us')
        org_id = ctx.obj.get('org_id')
        
        # Create client
        client = Rapid7Client(api_key, region, cache_manager)
        
        # Resolve investigation ID to RRN if provided
        resolved_target = target
        if investigation_id:
            resolved_target = resolve_investigation_id(client, investigation_id, region, config_manager, org_id)
        
        # Check if target is provided - API requires it
        if not resolved_target:
            console.print("[red]Error: Either --target or --investigation-id must be provided[/red]")
            console.print("[yellow]Usage examples:[/yellow]")
            console.print("  r7 siem investigation comment list --investigation-id NF9IQAF3YMUL")
            console.print("  r7 siem investigation comment list --target rrn:investigation:...")
            raise click.Abort()
        
        # Build query parameters
        params = {}
        if limit:
            params['size'] = limit
            
        # Fetch comments
        comments = client.list_comments(target=resolved_target, params=params)
        
        # Output
        if output == 'json':
            click.echo(json.dumps(comments, indent=2))
        elif output == 'simple':
            data = comments.get('data', []) or []
            for comment in data:
                comment_id = comment.get('rrn', 'N/A')
                if ':' in comment_id:
                    parts = comment_id.split(':')
                    if len(parts) >= 6:
                        comment_id = parts[-1]
                body_preview = comment.get('body', '')[:50] + ('...' if len(comment.get('body', '')) > 50 else '')
                click.echo(f"{comment_id}: {body_preview} [{comment.get('visibility', 'N/A')}]")
        else:  # table
            table = Table(title="Comments")
            table.add_column("ID", style="cyan")
            table.add_column("Body", style="green")
            table.add_column("Visibility", style="yellow")
            table.add_column("Author", style="blue")
            table.add_column("Created", style="dim")

            data = comments.get('data', []) or []
            for comment in data:
                comment_id = comment.get('rrn', 'N/A')
                if ':' in comment_id:
                    parts = comment_id.split(':')
                    if len(parts) >= 6:
                        comment_id = parts[-1]
                
                body = comment.get('body', 'N/A')
                if len(body) > 50:
                    body = body[:47] + '...'
                
                author = comment.get('creator', {}).get('name', 'Unknown')
                created_time = comment.get('created_time', 'N/A')
                if created_time != 'N/A' and 'T' in created_time:
                    created_time = created_time.split('T')[0]
                
                table.add_row(
                    comment_id,
                    body,
                    comment.get('visibility', 'N/A'),
                    author,
                    created_time
                )
            console.print(table)
            
    except Exception as e:
        console.print(f"[red]Error: {str(e)}[/red]")
        raise click.Abort()

@comment_group.command('create')
@click.argument('investigation_id')
@click.argument('body')
@click.option('--output', type=click.Choice(['simple', 'table', 'json']), default='simple',
              help='Output format')
@click.pass_context
def create_comment(ctx, investigation_id, body, output):
    """Create a comment on an investigation"""
    try:
        config_manager = ConfigManager()
        cache_manager = CacheManager()
        
        # Get credentials
        api_key = CredentialManager.get_api_key(ctx.obj.get('api_key'))
        if not api_key:
            raise AuthenticationError("API key not found. Use 'r7 config cred store' to save credentials.")
        
        region = ctx.obj.get('region') or config_manager.get('region', 'us')
        org_id = ctx.obj.get('org_id')
        client = Rapid7Client(api_key, region, cache_manager)
        
        # Resolve investigation ID to RRN
        resolved_target = resolve_investigation_id(client, investigation_id, region, config_manager, org_id)
        
        comment = client.create_comment(resolved_target, body)
        
        if output == 'json':
            click.echo(json.dumps(comment, indent=2))
        else:
            comment_data = comment if isinstance(comment, dict) else {}
            console.print(f"[green]✓ Comment created successfully[/green]")
            console.print(f"[bold cyan]RRN:[/bold cyan] {comment_data.get('rrn', 'N/A')}")
            console.print(f"[bold cyan]Target:[/bold cyan] {comment_data.get('target', 'N/A')}")
            
    except Exception as e:
        console.print(f"[red]Error: {str(e)}[/red]")
        raise click.Abort()

@comment_group.command('delete')
@click.argument('comment_rrn')
@click.option('--output', type=click.Choice(['simple', 'table', 'json']), default='simple',
              help='Output format')
@click.pass_context
def delete_comment(ctx, comment_rrn, output):
    """Delete a comment"""
    try:
        config_manager = ConfigManager()
        cache_manager = CacheManager()
        
        # Get credentials
        api_key = CredentialManager.get_api_key(ctx.obj.get('api_key'))
        if not api_key:
            raise AuthenticationError("API key not found. Use 'r7 config cred store' to save credentials.")
        
        region = ctx.obj.get('region') or config_manager.get('region', 'us')
        client = Rapid7Client(api_key, region, cache_manager)
        result = client.delete_comment(comment_rrn)
        
        if output == 'json':
            click.echo(json.dumps(result, indent=2))
        else:
            console.print(f"[green]✓ Comment deleted successfully[/green]")
            
    except Exception as e:
        console.print(f"[red]Error: {str(e)}[/red]")
        raise click.Abort()

# Alert functionality removed