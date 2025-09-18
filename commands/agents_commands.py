import click
import json
import sys
from rich.console import Console
from rich.table import Table
from rich.panel import Panel
from api.client import Rapid7Client
from utils.config import ConfigManager
from utils.cache import CacheManager
from utils.credentials import CredentialManager
from utils.exceptions import APIError, ConfigurationError, AuthenticationError

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
    """Determine if we should use JSON output based on pipe detection and user preference."""
    if output_format:
        return output_format == 'json'
    if not sys.stdout.isatty():
        return True
    return config_default == 'json'

@click.group()
def agents():
    """view insight agents (graphql preview)"""
    pass

@agents.command(name='list')
@click.option('--limit', type=int, default=1000, help='Maximum number of agents to return (default: 1000)')
@click.option('--output', type=click.Choice(['table', 'json']), help='Output format')
@click.option('--no-cache', is_flag=True, help='Disable caching for this request')
@click.pass_context
def list_agents(ctx, limit, output, no_cache):
    """list all agents status and version information"""
    client, config_manager = get_client_and_config(ctx)
    use_json = should_use_json_output(output, config_manager.get('default_output'))

    try:
        # Create cache key
        cache_key = f"agents_list_{limit}"

        cached_result = None
        if client.cache_manager and not no_cache:
            cached_result = client.cache_manager.get('agents', cache_key)
            if cached_result:
                if not use_json:
                    console.print("📋 Using cached result", style="dim")

        if not cached_result:
            agents = client.list_agents(limit=limit)
            if client.cache_manager and not no_cache:
                client.cache_manager.set('agents', cache_key, agents)
        else:
            agents = cached_result

        if use_json:
            output_data = {
                'agents': agents,
                'total_count': len(agents)
            }
            click.echo(json.dumps(output_data, indent=2))
        else:
            if not agents:
                console.print("No agents found", style="yellow")
                return

            # Create rich agents table with more useful info - show full Asset IDs
            table = Table(title="Insight Agents")
            table.add_column("Hostname", style="cyan", width=18)
            table.add_column("Asset ID", style="white", min_width=32, no_wrap=True)
            table.add_column("OS", style="blue", width=14)
            table.add_column("Private IP", style="white", width=12)
            table.add_column("Status", style="bold", width=8)

            for agent in agents:
                hostname = agent.get('hostname', 'Unknown')
                asset_id = agent.get('asset_id', 'Unknown')
                os_vendor = agent.get('os_vendor', '')
                os_version = agent.get('os_version', '')

                # Create concise OS display
                if os_vendor and os_version:
                    os_display = f"{os_vendor} {os_version}"
                elif agent.get('platform'):
                    os_display = agent.get('platform', 'Unknown')
                else:
                    os_display = 'Unknown'

                private_ip = agent.get('private_ip') or 'N/A'
                status = agent.get('agent_status') or 'Unknown'

                # Color code status
                if status.upper() == 'ONLINE':
                    status_colored = f"[green]{status}[/green]"
                elif status.upper() == 'OFFLINE':
                    status_colored = f"[red]{status}[/red]"
                elif status.upper() in ['STALE', 'WARNING']:
                    status_colored = f"[yellow]{status}[/yellow]"
                else:
                    status_colored = status

                # Show full values without truncation
                hostname_display = hostname[:17] + ('…' if len(hostname) > 17 else '')
                asset_id_display = asset_id  # Show full asset ID, no truncation
                os_display = os_display[:13] + ('…' if len(os_display) > 13 else '')

                table.add_row(
                    hostname_display,
                    asset_id_display,
                    os_display,
                    private_ip,
                    status_colored
                )

            console.print(table)
            console.print(f"\n[dim]📊 Found {len(agents)} agents[/dim]")

    except (APIError, Exception) as e:
        click.echo(f"❌ {e}", err=True)


@agents.command(name='status')
@click.option('--output', type=click.Choice(['table', 'json']), help='Output format')
@click.option('--no-cache', is_flag=True, help='Disable caching for this request')
@click.pass_context
def agent_status(ctx, output, no_cache):
    """show summary (online/offline/stale counts)"""
    client, config_manager = get_client_and_config(ctx)
    use_json = should_use_json_output(output, config_manager.get('default_output'))

    try:
        # Create cache key
        cache_key = "agents_status_summary"

        cached_result = None
        if client.cache_manager and not no_cache:
            cached_result = client.cache_manager.get('agents', cache_key)
            if cached_result:
                if not use_json:
                    console.print("📋 Using cached result", style="dim")

        if not cached_result:
            agents = client.list_agents(limit=10000)  # Get all agents for status summary
            if client.cache_manager and not no_cache:
                client.cache_manager.set('agents', cache_key, agents)
        else:
            agents = cached_result

        # Count agents by status
        status_counts = {}
        version_counts = {}

        for agent in agents:
            status = (agent['agent_status'] or 'Unknown').upper()
            status_counts[status] = status_counts.get(status, 0) + 1

            version = agent['agent_version'] or 'Unknown'
            version_counts[version] = version_counts.get(version, 0) + 1

        total_agents = len(agents)

        if use_json:
            output_data = {
                'total_agents': total_agents,
                'status_breakdown': status_counts,
                'version_breakdown': version_counts
            }
            click.echo(json.dumps(output_data, indent=2))
        else:
            if total_agents == 0:
                console.print("No agents found", style="yellow")
                return

            # Agent Status Summary Panel
            online = status_counts.get('ONLINE', 0)
            offline = status_counts.get('OFFLINE', 0)
            stale = status_counts.get('STALE', 0)
            warning = status_counts.get('WARNING', 0)
            unknown = status_counts.get('UNKNOWN', 0)

            status_text = f"[green]✓ {online} Online[/green]"
            if offline > 0:
                status_text += f" | [red]✗ {offline} Offline[/red]"
            if stale > 0:
                status_text += f" | [yellow]⚠ {stale} Stale[/yellow]"
            if warning > 0:
                status_text += f" | [orange1]⚠ {warning} Warning[/orange1]"
            if unknown > 0:
                status_text += f" | [dim]? {unknown} Unknown[/dim]"

            console.print(Panel(
                f"[bold]Total Agents: {total_agents}[/bold]\n{status_text}",
                title="[bold cyan]Agent Status Summary[/bold cyan]",
                expand=False
            ))

            # Version breakdown table if there are multiple versions
            if len(version_counts) > 1:
                console.print()
                version_table = Table(title="Agent Versions", width=60)
                version_table.add_column("Version", style="blue")
                version_table.add_column("Count", style="cyan")
                version_table.add_column("Percentage", style="green")

                for version, count in sorted(version_counts.items(), key=lambda x: x[1], reverse=True):
                    percentage = (count / total_agents) * 100
                    version_table.add_row(
                        version,
                        str(count),
                        f"{percentage:.1f}%"
                    )

                console.print(version_table)

    except (APIError, Exception) as e:
        click.echo(f"❌ {e}", err=True)


@agents.command(name='show')
@click.argument('asset_id')
@click.option('--output', type=click.Choice(['table', 'json']), help='Output format')
@click.option('--no-cache', is_flag=True, help='Disable caching for this request')
@click.pass_context
def show_agent(ctx, asset_id, output, no_cache):
    """get agent details from an ID"""
    client, config_manager = get_client_and_config(ctx)
    use_json = should_use_json_output(output, config_manager.get('default_output'))

    try:
        # Create cache key
        cache_key = f"agent_details_{asset_id}"

        cached_result = None
        if client.cache_manager and not no_cache:
            cached_result = client.cache_manager.get('agents', cache_key)
            if cached_result:
                if not use_json:
                    console.print("📋 Using cached result", style="dim")

        if not cached_result:
            agent = client.get_agent_details(asset_id)
            if client.cache_manager and not no_cache:
                client.cache_manager.set('agents', cache_key, agent)
        else:
            agent = cached_result

        if use_json:
            click.echo(json.dumps(agent, indent=2))
        else:
            # Detailed agent information
            status = (agent['agent_status'] or 'Unknown').upper()

            # Color code status
            if status == 'ONLINE':
                status_display = f"[green]{status}[/green]"
            elif status == 'OFFLINE':
                status_display = f"[red]{status}[/red]"
            elif status in ['STALE', 'WARNING']:
                status_display = f"[yellow]{status}[/yellow]"
            else:
                status_display = status

            hostname = agent.get('hostname', 'Unknown')
            detail_table = Table(title=f"Agent Details: {hostname}", width=100)
            detail_table.add_column("Property", style="cyan", width=20)
            detail_table.add_column("Value", style="white", width=75)

            # Basic info
            detail_table.add_row("Hostname", hostname)
            detail_table.add_row("Platform", agent.get('platform', 'Unknown'))
            detail_table.add_row("OS Description", agent.get('os_description', 'N/A'))
            detail_table.add_row("Host Type", agent.get('host_type', 'Unknown'))

            # Network info
            detail_table.add_row("Private IP", agent.get('private_ip', 'N/A'))
            detail_table.add_row("Public IP", agent.get('public_ip', 'N/A'))
            detail_table.add_row("MAC Address", agent.get('mac_address', 'N/A'))

            # Agent info
            detail_table.add_row("Agent Version", agent['agent_version'] or 'Unknown')
            detail_table.add_row("Agent Status", status_display)
            detail_table.add_row("Deploy Time", agent.get('deploy_time', 'N/A'))
            detail_table.add_row("Last Update", agent.get('last_update', 'N/A'))

            # IDs
            detail_table.add_row("Asset ID", agent['asset_id'])
            detail_table.add_row("Agent ID", agent['agent_id'])

            console.print(detail_table)

    except (APIError, Exception) as e:
        click.echo(f"❌ {e}", err=True)