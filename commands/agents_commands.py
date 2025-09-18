import click
import json
from rich.console import Console
from rich.table import Table
from rich.panel import Panel
from utils.cli import ClientManager, OutputFormatter
from utils.exceptions import APIError

console = Console()

@click.group(name='agents')
def agents_group():
    """view insight agents (graphql preview)"""
    pass

@agents_group.command(name='list')
@click.option('--limit', type=int, default=1000, help='Maximum number of agents to return (default: 1000)')
@click.option('--output', type=click.Choice(['table', 'json']), help='Output format')
@click.option('--no-cache', is_flag=True, help='Disable caching for this request')
@click.pass_context
def list_agents(ctx, limit, output, no_cache):
    """list all agents status and version information"""
    try:
        client, config_manager = ClientManager().get_client_and_config(ctx, cache_namespace='agents')

        # Create cache key
        cache_key = f"agents_list_{limit}"

        cached_result = None
        if client.cache_manager and not no_cache:
            cached_result = client.cache_manager.get('agents', cache_key)
            if cached_result:
                OutputFormatter.display_cached_message()

        if not cached_result:
            agents = client.list_agents(limit=limit)
            if client.cache_manager and not no_cache:
                client.cache_manager.set('agents', cache_key, agents)
        else:
            agents = cached_result

        # Create table formatter for agents list
        def table_fmt(data):
            if not data['agents']:
                console.print("No agents found", style="yellow")
                return

            # Create rich agents table with more useful info - show full Asset IDs
            table = Table(title="Insight Agents")
            table.add_column("Hostname", style="cyan", width=18)
            table.add_column("Asset ID", style="white", min_width=32, no_wrap=True)
            table.add_column("OS", style="blue", width=14)
            table.add_column("Private IP", style="white", width=12)
            table.add_column("Status", style="bold", width=8)

            for agent in data['agents']:
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
            console.print(f"\n[dim]📊 Found {data['total_count']} agents[/dim]")

        output_data = {
            'agents': agents,
            'total_count': len(agents)
        }
        OutputFormatter.output_data(output_data, output, config_manager, table_formatter=table_fmt)

    except APIError as e:
        OutputFormatter.display_error(e, "Failed to list agents")
        ctx.exit(1)


@agents_group.command(name='status')
@click.option('--output', type=click.Choice(['table', 'json']), help='Output format')
@click.option('--no-cache', is_flag=True, help='Disable caching for this request')
@click.pass_context
def agent_status(ctx, output, no_cache):
    """show summary (online/offline/stale counts)"""
    try:
        client, config_manager = ClientManager().get_client_and_config(ctx, cache_namespace='agents')

        # Create cache key
        cache_key = "agents_status_summary"

        cached_result = None
        if client.cache_manager and not no_cache:
            cached_result = client.cache_manager.get('agents', cache_key)
            if cached_result:
                OutputFormatter.display_cached_message()

        if not cached_result:
            agents = client.list_agents(limit=1000)  # Get agents for status summary
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

        def table_fmt(data):
            if data['total_agents'] == 0:
                console.print("No agents found", style="yellow")
                return

            # Agent Status Summary Panel
            status_breakdown = data['status_breakdown']
            online = status_breakdown.get('ONLINE', 0)
            offline = status_breakdown.get('OFFLINE', 0)
            stale = status_breakdown.get('STALE', 0)
            warning = status_breakdown.get('WARNING', 0)
            unknown = status_breakdown.get('UNKNOWN', 0)

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
                f"[bold]Total Agents: {data['total_agents']}[/bold]\n{status_text}",
                title="[bold cyan]Agent Status Summary[/bold cyan]",
                expand=False
            ))

            # Version breakdown table if there are multiple versions
            version_breakdown = data['version_breakdown']
            if len(version_breakdown) > 1:
                console.print()
                version_table = Table(title="Agent Versions", width=60)
                version_table.add_column("Version", style="blue")
                version_table.add_column("Count", style="cyan")
                version_table.add_column("Percentage", style="green")

                for version, count in sorted(version_breakdown.items(), key=lambda x: x[1], reverse=True):
                    percentage = (count / data['total_agents']) * 100
                    version_table.add_row(
                        version,
                        str(count),
                        f"{percentage:.1f}%"
                    )

                console.print(version_table)

        output_data = {
            'total_agents': total_agents,
            'status_breakdown': status_counts,
            'version_breakdown': version_counts
        }
        OutputFormatter.output_data(output_data, output, config_manager, table_formatter=table_fmt)

    except APIError as e:
        OutputFormatter.display_error(e, "Failed to get agent status")
        ctx.exit(1)


@agents_group.command(name='show')
@click.argument('asset_id')
@click.option('--output', type=click.Choice(['table', 'json']), help='Output format')
@click.option('--no-cache', is_flag=True, help='Disable caching for this request')
@click.pass_context
def show_agent(ctx, asset_id, output, no_cache):
    """get agent details from an ID"""
    try:
        client, config_manager = ClientManager().get_client_and_config(ctx, cache_namespace='agents')

        # Create cache key
        cache_key = f"agent_details_{asset_id}"

        cached_result = None
        if client.cache_manager and not no_cache:
            cached_result = client.cache_manager.get('agents', cache_key)
            if cached_result:
                OutputFormatter.display_cached_message()

        if not cached_result:
            agent = client.get_agent_details(asset_id)
            if client.cache_manager and not no_cache:
                client.cache_manager.set('agents', cache_key, agent)
        else:
            agent = cached_result

        def table_fmt(data):
            # Detailed agent information
            status = (data['agent_status'] or 'Unknown').upper()

            # Color code status
            if status == 'ONLINE':
                status_display = f"[green]{status}[/green]"
            elif status == 'OFFLINE':
                status_display = f"[red]{status}[/red]"
            elif status in ['STALE', 'WARNING']:
                status_display = f"[yellow]{status}[/yellow]"
            else:
                status_display = status

            hostname = data.get('hostname', 'Unknown')
            detail_table = Table(title=f"Agent Details: {hostname}", width=100)
            detail_table.add_column("Property", style="cyan", width=20)
            detail_table.add_column("Value", style="white", width=75)

            # Basic info
            detail_table.add_row("Hostname", hostname)
            detail_table.add_row("Platform", data.get('platform', 'Unknown'))
            detail_table.add_row("OS Description", data.get('os_description', 'N/A'))
            detail_table.add_row("Host Type", data.get('host_type', 'Unknown'))

            # Network info
            detail_table.add_row("Private IP", data.get('private_ip', 'N/A'))
            detail_table.add_row("Public IP", data.get('public_ip', 'N/A'))
            detail_table.add_row("MAC Address", data.get('mac_address', 'N/A'))

            # Agent info
            detail_table.add_row("Agent Version", data['agent_version'] or 'Unknown')
            detail_table.add_row("Agent Status", status_display)
            detail_table.add_row("Deploy Time", data.get('deploy_time', 'N/A'))
            detail_table.add_row("Last Update", data.get('last_update', 'N/A'))

            # IDs
            detail_table.add_row("Asset ID", data['asset_id'])
            detail_table.add_row("Agent ID", data['agent_id'])

            console.print(detail_table)

        OutputFormatter.output_data(agent, output, config_manager, table_formatter=table_fmt)

    except APIError as e:
        OutputFormatter.display_error(e, "Failed to get agent details")
        ctx.exit(1)