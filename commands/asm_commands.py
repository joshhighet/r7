import json
import sys
import subprocess
import click
from rich.console import Console
from rich.table import Table
from rich.progress import Progress, SpinnerColumn, TextColumn, TimeElapsedColumn
from api.client import Rapid7Client
from utils.cli import ClientManager
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

def _parse_return_clause(query):
    """Parse RETURN clause from Cypher query to extract column names.
    Returns a list of column header names.
    """
    import re
    
    # Find the RETURN clause (case insensitive)
    return_match = re.search(r'\bRETURN\s+(.+?)(?:\s+(?:ORDER\s+BY|SKIP|LIMIT)|$)', query, re.IGNORECASE | re.DOTALL)
    if not return_match:
        return []
    
    return_clause = return_match.group(1).strip()
    
    # Split by commas (but not within parentheses)
    columns = []
    current = []
    paren_depth = 0
    bracket_depth = 0
    
    for char in return_clause:
        if char == '(' and bracket_depth == 0:
            paren_depth += 1
            current.append(char)
        elif char == ')' and bracket_depth == 0:
            paren_depth -= 1
            current.append(char)
        elif char == '[':
            bracket_depth += 1
            current.append(char)
        elif char == ']':
            bracket_depth -= 1
            current.append(char)
        elif char == ',' and paren_depth == 0 and bracket_depth == 0:
            columns.append(''.join(current).strip())
            current = []
        else:
            current.append(char)
    
    if current:
        columns.append(''.join(current).strip())
    
    # Extract column names from each column expression
    header_names = []
    for col in columns:
        col = col.strip()
        
        # Check for alias (e.g., "count(*) as count")
        alias_match = re.search(r'\s+[Aa][Ss]\s+(\w+)$', col)
        if alias_match:
            header_names.append(alias_match.group(1))
        # Check for property access (e.g., "s.service_port")
        elif '.' in col and '(' not in col:
            # Extract property name after the last dot
            parts = col.split('.')
            header_names.append(parts[-1].strip())
        # Check for simple identifier (e.g., "u" or "m")
        elif re.match(r'^[a-zA-Z_]\w*$', col):
            header_names.append(col)
        # Check for function calls without alias (e.g., "count(m)")
        elif '(' in col:
            # Try to extract function name
            func_match = re.match(r'^(\w+)\s*\(', col)
            if func_match:
                header_names.append(func_match.group(1))
            else:
                header_names.append(f"Value {len(header_names) + 1}")
        else:
            # Fallback to generic name
            header_names.append(f"Value {len(header_names) + 1}")
    
    return header_names

def should_use_json_output(output_format, config_default):
    """Determine if we should use JSON output based on pipe detection and user preference"""
    if output_format:
        return output_format == 'json'
    if not sys.stdout.isatty():
        return True
    return config_default == 'json'

def get_client_and_config(ctx, api_key=None):
    """Use shared ClientManager to acquire (client, config)."""
    return ClientManager().get_client_and_config(ctx, api_key=api_key)

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
        base_url = client.get_base_url('asm_profile')
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
        click.echo(f"Error: {e}", err=True)

@asm_group.group(name='apps')
def apps_group():
    """Surface Command apps management"""
    pass

@apps_group.command(name='list')
@click.option('--output', type=click.Choice(['table', 'json']), help='Output format')
@click.option('--no-cache', is_flag=True, help='Disable caching for this request')
@click.option('--all-types', is_flag=True, help='Show all types instead of truncating to first 3')
@click.option('--exclude-apps', help='Comma-separated list of app IDs to exclude from output')
@click.option('--full-output', is_flag=True, help='Include all fields in JSON output (default shows minimal fields matching table view)')
@click.pass_context
def apps_list(ctx, output, no_cache, all_types, exclude_apps, full_output):
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
            # Create minimal output by default to save context window space
            if full_output:
                # Full output - include all fields
                click.echo(json.dumps(data, separators=(',', ':')))
            else:
                # Minimal output - only include fields shown in table view
                minimal_data = {}
                for app_id, app_data in data.items():
                    # Extract only the fields shown in the table
                    minimal_app = {
                        'id': app_id,
                        'name': app_data.get('name', 'Unknown'),
                        'version': app_data.get('version', 'Unknown'),
                        'types': app_data.get('types', [])
                    }
                    
                    # Add created date if available
                    metadata = app_data.get('stored_object_metadata', {})
                    if metadata.get('created'):
                        minimal_app['created'] = metadata['created']
                    
                    minimal_data[app_id] = minimal_app
                
                click.echo(json.dumps(minimal_data, separators=(',', ':')))
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
        click.echo(f"Error: {e}", err=True)

@apps_group.command(name='health')
@click.option('--output', type=click.Choice(['table', 'json']), help='Output format')
@click.option('--no-cache', is_flag=True, help='Disable caching for this request')
@click.option('--show-all', is_flag=True, help='Show all apps including those without profiles')
@click.pass_context
def apps_health(ctx, output, no_cache, show_all):
    """Show Surface Command apps health status"""
    client, config_manager = get_client_and_config(ctx)
    use_json = should_use_json_output(output, config_manager.get('default_output'))

    try:
        # Get app statuses and profiles in parallel
        cache_enabled = client.cache_manager and not no_cache

        # Fetch app statuses
        status_cache_key = "apps_status"
        app_statuses = None
        if cache_enabled:
            app_statuses = client.cache_manager.get('apps_health', status_cache_key)

        if not app_statuses:
            base_url = client.get_base_url('asm_apps')
            status_response = client.make_request('GET', f"{base_url}/apps/info/status")
            if status_response.status_code != 200:
                raise APIError(f"Failed to get app statuses: {status_response.status_code} - {status_response.text}")
            app_statuses = status_response.json()
            if cache_enabled:
                client.cache_manager.set('apps_health', status_cache_key, app_statuses)

        # Fetch profiles
        profiles_cache_key = "apps_profiles"
        profiles = None
        if cache_enabled:
            profiles = client.cache_manager.get('apps_health', profiles_cache_key)

        if not profiles:
            base_url = client.get_base_url('asm_apps')
            profiles_response = client.make_request('GET', f"{base_url}/profiles")
            if profiles_response.status_code != 200:
                raise APIError(f"Failed to get profiles: {profiles_response.status_code} - {profiles_response.text}")
            profiles = profiles_response.json()
            if cache_enabled:
                client.cache_manager.set('apps_health', profiles_cache_key, profiles)

        # Fetch recent execution status for profiles
        execution_cache_key = "apps_executions"
        execution_data = {}
        execution_issues = {}
        if cache_enabled:
            cached = client.cache_manager.get('apps_health', execution_cache_key) or {}
            execution_data = cached.get('data', {})
            execution_issues = cached.get('issues', {})

        if not execution_data:
            try:
                # Get recent executions for all workflows
                workflow_base = f"https://{client.region}.api.insight.rapid7.com/surface/workflow-api"
                exec_response = client.make_request('GET', f"{workflow_base}/executions?size=50")

                if exec_response.status_code == 200:
                    executions = exec_response.json()

                    # Track latest execution status per profile
                    latest_by_profile = {}

                    for execution in executions:
                        workflow_id = execution.get('samos_workflow_id', '')
                        if '/' in workflow_id:
                            profile_id = workflow_id.split('/')[0]
                            timestamp = execution.get('timestamp', '')
                            status = execution.get('status', 'unknown')
                            exec_id = execution.get('execution_id', '')

                            # Keep only the latest execution per profile
                            if profile_id not in latest_by_profile or timestamp > latest_by_profile[profile_id]['timestamp']:
                                latest_by_profile[profile_id] = {
                                    'status': status,
                                    'timestamp': timestamp,
                                    'execution_id': exec_id,
                                    'workflow_id': workflow_id,
                                    'error_msg': execution.get('workflow_err_msg', execution.get('zeebe_err_msg', '')),
                                    'duration_seconds': None
                                }

                    # Calculate duration for completed/failed executions (limit to avoid too many API calls)
                    # Only calculate for the first 10 most recent profiles
                    sorted_profiles = sorted(latest_by_profile.items(),
                                           key=lambda x: x[1]['timestamp'], reverse=True)[:10]

                    for profile_id, exec_info in sorted_profiles:
                        exec_id = exec_info['execution_id']
                        if exec_id:
                            try:
                                # Get logs to calculate duration
                                logs_response = client.make_request('GET',
                                    f"{workflow_base}/executions/{exec_id}/logs?only_user_msgs=true&size=1000&offset=0")

                                if logs_response.status_code == 200:
                                    logs = logs_response.json()
                                    if len(logs) >= 2:
                                        from datetime import datetime
                                        start_time = datetime.fromisoformat(logs[0]['timestamp'].replace('Z', '+00:00'))
                                        end_time = datetime.fromisoformat(logs[-1]['timestamp'].replace('Z', '+00:00'))
                                        duration = (end_time - start_time).total_seconds()
                                        exec_info['duration_seconds'] = duration
                            except:
                                pass  # Duration calculation is best-effort

                    execution_data = latest_by_profile

                    # Find profiles with execution failures
                    for profile_id, exec_data in latest_by_profile.items():
                        if exec_data['status'] in ['error', 'failed', 'timeout']:
                            execution_issues[profile_id] = exec_data

                    if cache_enabled:
                        client.cache_manager.set('apps_health', execution_cache_key, {
                            'data': execution_data,
                            'issues': execution_issues
                        })

            except Exception as e:
                if not use_json:
                    console.print(f"[dim yellow]Could not check execution status: {e}[/dim yellow]")

        # Process data
        status_by_app = {status['id']: status['statuses'] for status in app_statuses}
        profiles_by_app = {}
        profile_status_counts = {}
        orchestrator_ids = set()

        for profile in profiles:
            app_id = profile.get('integration_id', 'unknown')
            status = profile.get('status', 'unknown')
            location = profile.get('location', {})

            if app_id not in profiles_by_app:
                profiles_by_app[app_id] = []
            profiles_by_app[app_id].append(profile)

            if status not in profile_status_counts:
                profile_status_counts[status] = 0
            profile_status_counts[status] += 1

            if location.get('type') == 'orchestrator' and location.get('id'):
                orchestrator_ids.add(location['id'])

        # Count apps with issues
        apps_with_issues = [app_id for app_id, statuses in status_by_app.items() if 'ok' not in statuses]
        profiles_with_issues = [p for p in profiles if p.get('status') != 'configured']
        profiles_with_exec_issues = [p for p in profiles if p.get('id') in execution_issues]

        # Safe defaults - data-only apps that don't need connector profiles
        safe_defaults = {
            'cisa.exploit.app', 'first.epss.app', 'mitre.attack.app', 'mitre.cwe.app',
            'nist.nvd.app', 'noetic.builtins.app', 'noetic.dashboard.app', 'noetic.ml.app',
            'rapid7.command_platform.app', 'combined.vuln.app'
        }

        apps_without_profiles = [
            app_id for app_id in status_by_app.keys()
            if app_id not in profiles_by_app and app_id not in safe_defaults
        ]

        if use_json:
            health_data = {
                'summary': {
                    'total_apps': len(status_by_app),
                    'apps_ok': len(status_by_app) - len(apps_with_issues),
                    'apps_with_issues': len(apps_with_issues),
                    'total_profiles': len(profiles),
                    'profiles_configured': profile_status_counts.get('configured', 0),
                    'profiles_with_issues': len(profiles_with_issues),
                    'profiles_with_exec_issues': len(profiles_with_exec_issues),
                    'apps_without_profiles': len(apps_without_profiles),
                    'orchestrators': len(orchestrator_ids)
                },
                'apps_with_issues': apps_with_issues,
                'profiles_with_issues': profiles_with_issues,
                'profiles_with_exec_issues': profiles_with_exec_issues,
                'execution_issues': execution_issues,
                'apps_without_profiles': apps_without_profiles,
                'profiles_by_app': profiles_by_app if show_all else {k: v for k, v in profiles_by_app.items() if len(v) > 0}
            }
            click.echo(json.dumps(health_data, indent=2))
        else:
            # Display summary
            console.print("[bold]ASM Apps Health Status[/bold]\n")

            # Overview section
            console.print("[bold cyan]OVERVIEW[/bold cyan]")
            total_apps = len(status_by_app)
            apps_ok = total_apps - len(apps_with_issues)
            console.print(f"  • Total Apps: {total_apps} ({apps_ok} ok, {len(apps_with_issues)} issues)")

            total_profiles = len(profiles)
            profiles_ok = profile_status_counts.get('configured', 0)
            profiles_issues = total_profiles - profiles_ok

            # Execution health summary
            profiles_exec_healthy = total_profiles - len(profiles_with_exec_issues)
            exec_failed = len(profiles_with_exec_issues)

            console.print(f"  • Total Profiles: {total_profiles} ({profiles_ok} configured, {profiles_issues} config issues)")
            console.print(f"  • Executions: {profiles_exec_healthy} healthy, {exec_failed} failed")
            console.print(f"  • Orchestrators: {len(orchestrator_ids)} active")
            console.print()

            # Issues section
            if apps_with_issues or profiles_with_issues or profiles_with_exec_issues:
                console.print("[bold red]ISSUES[/bold red]")
                if apps_with_issues:
                    console.print(f"  Apps with issues ({len(apps_with_issues)}):")
                    for app_id in apps_with_issues:
                        statuses = ', '.join(status_by_app[app_id])
                        console.print(f"    • {app_id}: {statuses}")

                if profiles_with_issues:
                    console.print(f"  Profiles with configuration issues ({len(profiles_with_issues)}):")
                    for profile in profiles_with_issues:
                        console.print(f"    • {profile['name']} ({profile['integration_id']}): {profile['status']}")

                if profiles_with_exec_issues:
                    console.print(f"  Profiles with execution failures ({len(profiles_with_exec_issues)}):")
                    for profile in profiles_with_exec_issues:
                        profile_id = profile.get('id')
                        exec_data = execution_issues.get(profile_id, {})
                        error_msg = exec_data.get('error_msg', 'Unknown error')
                        timestamp = exec_data.get('timestamp', '')
                        # Truncate long error messages
                        short_error = error_msg[:100] + '...' if len(error_msg) > 100 else error_msg
                        console.print(f"    • {profile['name']} ({profile['integration_id']}): {short_error}")
                        if timestamp:
                            from datetime import datetime
                            try:
                                dt = datetime.fromisoformat(timestamp.replace('Z', '+00:00'))
                                time_str = dt.strftime('%Y-%m-%d %H:%M UTC')
                                console.print(f"      Last failed: {time_str}")
                            except:
                                pass
                console.print()
            else:
                console.print("[bold green]NO ISSUES DETECTED[/bold green]\n")

            # Profiles table - show individual profiles for better visibility
            if profiles_by_app:
                console.print("[bold cyan]CONNECTOR PROFILES[/bold cyan]")
                table = Table()
                table.add_column("Profile Name", style="white", width=30)
                table.add_column("App", style="cyan", width=25)
                table.add_column("Config", style="green", width=12)
                table.add_column("Execution", style="blue", width=20)
                table.add_column("Location", style="dim", width=12)

                excluded_from_display = {
                    'rapid7.command_platform.app', 'combined.vuln.app'
                }

                all_profiles = []
                for app_id, app_profiles in profiles_by_app.items():
                    if app_id not in excluded_from_display:
                        for profile in app_profiles:
                            profile['app_id'] = app_id
                            all_profiles.append(profile)

                # Sort: execution issues first, then config issues, then healthy
                def profile_sort_key(profile):
                    profile_id = profile.get('id')
                    has_exec_issue = profile_id in execution_issues
                    has_config_issue = profile.get('status') != 'configured'
                    return (not has_exec_issue, not has_config_issue, profile.get('name', ''))

                sorted_profiles = sorted(all_profiles, key=profile_sort_key)

                for profile in sorted_profiles:
                    profile_id = profile.get('id')
                    profile_name = profile.get('name', 'Unknown')
                    app_id = profile.get('app_id', '')
                    config_status = profile.get('status', 'unknown')

                    # Format location display
                    location = profile.get('location', {})
                    if location.get('type') == 'saas':
                        location_display = 'SaaS'
                    elif location.get('type') == 'orchestrator' and location.get('id'):
                        orch_id = location['id']
                        location_display = f"{orch_id[:3]}...{orch_id[-5:]}"
                    else:
                        location_display = 'unknown'

                    # Config status with text indicators
                    if config_status == 'configured':
                        config_display = "configured"
                    else:
                        config_display = f"error: {config_status}"

                    # Execution status with duration
                    exec_info = execution_data.get(profile_id, {})
                    duration_seconds = exec_info.get('duration_seconds')

                    if profile_id in execution_issues:
                        exec_data = execution_issues[profile_id]
                        timestamp = exec_data.get('timestamp', '')
                        if timestamp:
                            try:
                                from datetime import datetime, timezone
                                dt = datetime.fromisoformat(timestamp.replace('Z', '+00:00'))
                                now = datetime.now(timezone.utc)
                                diff = now - dt

                                if diff.days > 0:
                                    time_ago = f"{diff.days}d ago"
                                elif diff.seconds > 3600:
                                    hours = diff.seconds // 3600
                                    time_ago = f"{hours}h ago"
                                else:
                                    minutes = diff.seconds // 60
                                    time_ago = f"{minutes}m ago"

                                exec_display = f"failed {time_ago}"
                            except:
                                exec_display = "failed"
                        else:
                            exec_display = "failed"
                    elif exec_info:
                        # Has execution data - format with duration if available
                        if duration_seconds is not None:
                            if duration_seconds < 60:
                                duration_str = f"{int(duration_seconds)}s"
                            elif duration_seconds < 3600:
                                duration_str = f"{int(duration_seconds / 60)}m"
                            else:
                                duration_str = f"{int(duration_seconds / 3600)}h{int((duration_seconds % 3600) / 60)}m"
                            exec_display = f"healthy ({duration_str})"
                        else:
                            exec_display = "healthy"
                    else:
                        exec_display = "healthy"

                    # Highlight problematic profiles
                    if profile_id in execution_issues or config_status != 'configured':
                        profile_name_display = f"[bold red]{profile_name}[/bold red]"
                        app_display = f"[red]{app_id}[/red]"
                    else:
                        profile_name_display = profile_name
                        app_display = app_id

                    table.add_row(
                        profile_name_display,
                        app_display,
                        config_display,
                        exec_display,
                        location_display
                    )

                console.print(table)
                console.print()

            # Apps without profiles
            if apps_without_profiles and (show_all or len(apps_without_profiles) <= 10):
                console.print("[bold cyan]APPS WITHOUT PROFILES[/bold cyan]")
                console.print(f"  Found {len(apps_without_profiles)} apps without connector profiles:")
                for i, app_id in enumerate(sorted(apps_without_profiles), 1):
                    if i <= 10 or show_all:
                        console.print(f"    • {app_id}")
                    elif i == 11:
                        console.print(f"    ... and {len(apps_without_profiles) - 10} more (use --show-all)")
                        break
                console.print()
            elif apps_without_profiles:
                console.print(f"[bold cyan]APPS WITHOUT PROFILES[/bold cyan]: {len(apps_without_profiles)} found (use --show-all to list)")
                console.print()

        if cache_enabled and not use_json:
            console.print("[dim]Data cached for faster subsequent requests[/dim]")

    except (APIError, ConfigurationError) as e:
        click.echo(f"Error: {e}", err=True)

@apps_group.group(name='runlogs')
def runlogs_group():
    """View connector execution run logs"""
    pass

@runlogs_group.command(name='list')
@click.option('--profile', help='Filter by profile ID')
@click.option('--limit', type=int, default=20, help='Number of executions to return')
@click.option('--output', type=click.Choice(['table', 'json']), help='Output format')
@click.pass_context
def runlogs_list(ctx, profile, limit, output):
    """List execution runs (all or filtered by --profile)"""
    client, config_manager = get_client_and_config(ctx)
    use_json = should_use_json_output(output, config_manager.get('default_output'))

    try:
        workflow_base = f"https://{client.region}.api.insight.rapid7.com/surface/workflow-api"

        # Get executions
        response = client.make_request('GET', f"{workflow_base}/executions?size={limit}")

        if response.status_code != 200:
            raise APIError(f"Failed to get executions: {response.status_code} - {response.text}")

        executions = response.json()

        # Filter by profile if specified
        if profile:
            executions = [e for e in executions if e.get('samos_workflow_id', '').startswith(f"{profile}/")]

        if use_json:
            click.echo(json.dumps(executions, indent=2))
        else:
            if not executions:
                console.print("No execution runs found", style="yellow")
                return

            table = Table(title=f"Execution Runs{' for ' + profile if profile else ''}")
            table.add_column("Execution ID", style="cyan", width=36)
            table.add_column("Profile ID", style="white", width=36)
            table.add_column("App", style="blue", width=20)
            table.add_column("Status", style="green", width=10)
            table.add_column("Timestamp", style="dim", width=20)

            for execution in executions:
                exec_id = execution.get('execution_id', '')
                workflow_id = execution.get('samos_workflow_id', '')
                status = execution.get('status', 'unknown')
                timestamp = execution.get('timestamp', '')

                # Parse workflow ID
                parts = workflow_id.split('/')
                exec_profile_id = parts[0] if len(parts) > 0 else ''
                app_id = parts[1] if len(parts) > 1 else ''

                # Format timestamp
                if timestamp:
                    try:
                        from datetime import datetime
                        dt = datetime.fromisoformat(timestamp.replace('Z', '+00:00'))
                        time_display = dt.strftime('%Y-%m-%d %H:%M')
                    except:
                        time_display = timestamp[:16]
                else:
                    time_display = ''

                # Color status
                if status == 'completed':
                    status_display = f"[green]{status}[/green]"
                elif status in ['error', 'failed']:
                    status_display = f"[red]{status}[/red]"
                else:
                    status_display = status

                table.add_row(exec_id, exec_profile_id, app_id, status_display, time_display)

            console.print(table)
            console.print(f"\n[dim]Showing {len(executions)} execution(s)[/dim]")

    except (APIError, ConfigurationError) as e:
        click.echo(f"Error: {e}", err=True)

@runlogs_group.command(name='show')
@click.argument('run_id')
@click.option('--output', type=click.Choice(['table', 'json']), help='Output format')
@click.pass_context
def runlogs_show(ctx, run_id, output):
    """Show execution details for specific run ID"""
    client, config_manager = get_client_and_config(ctx)
    use_json = should_use_json_output(output, config_manager.get('default_output'))

    try:
        workflow_base = f"https://{client.region}.api.insight.rapid7.com/surface/workflow-api"
        exec_id = run_id

        # Get execution logs
        logs_response = client.make_request('GET',
            f"{workflow_base}/executions/{exec_id}/logs?only_user_msgs=true&size=1000&offset=0")

        if logs_response.status_code != 200:
            raise APIError(f"Failed to get execution logs: {logs_response.status_code} - {logs_response.text}")

        logs = logs_response.json()

        # Get execution metadata
        exec_response = client.make_request('GET', f"{workflow_base}/executions?size=100")
        execution = None
        if exec_response.status_code == 200:
            all_execs = exec_response.json()
            execution = next((e for e in all_execs if e['execution_id'] == exec_id), None)

        if use_json:
            output_data = {
                'execution': execution,
                'logs': logs
            }
            click.echo(json.dumps(output_data, indent=2))
        else:
            if execution:
                console.print(f"[bold cyan]Execution Details[/bold cyan]\n")
                console.print(f"Execution ID: {exec_id}")
                console.print(f"Status: {execution.get('status', 'unknown')}")
                console.print(f"Workflow: {execution.get('samos_workflow_id', '')}")
                console.print(f"Timestamp: {execution.get('timestamp', '')}")

                # Calculate duration if we have logs
                if len(logs) >= 2:
                    try:
                        from datetime import datetime
                        start_time = datetime.fromisoformat(logs[0]['timestamp'].replace('Z', '+00:00'))
                        end_time = datetime.fromisoformat(logs[-1]['timestamp'].replace('Z', '+00:00'))
                        duration = (end_time - start_time).total_seconds()

                        if duration < 60:
                            duration_str = f"{int(duration)}s"
                        elif duration < 3600:
                            duration_str = f"{int(duration / 60)}m {int(duration % 60)}s"
                        else:
                            duration_str = f"{int(duration / 3600)}h {int((duration % 3600) / 60)}m"

                        console.print(f"Duration: {duration_str}")
                    except:
                        pass

                console.print()

            # Display logs
            if logs:
                console.print(f"[bold cyan]Execution Logs[/bold cyan] ({len(logs)} entries)\n")

                for log in logs:
                    timestamp = log.get('timestamp', '')
                    content = log.get('content', {})
                    message = content.get('message', '')
                    level = content.get('level', 'INFO')

                    # Format timestamp
                    try:
                        from datetime import datetime
                        dt = datetime.fromisoformat(timestamp.replace('Z', '+00:00'))
                        time_str = dt.strftime('%H:%M:%S')
                    except:
                        time_str = timestamp[:8] if timestamp else ''

                    # Color by level
                    if level == 'ERROR':
                        console.print(f"[red]{time_str}[/red] {message}")
                    elif level == 'WARNING':
                        console.print(f"[yellow]{time_str}[/yellow] {message}")
                    else:
                        console.print(f"[dim]{time_str}[/dim] {message}")
            else:
                console.print("No logs found", style="yellow")

    except (APIError, ConfigurationError) as e:
        click.echo(f"Error: {e}", err=True)

@runlogs_group.command(name='latest')
@click.argument('profile_id')
@click.option('--output', type=click.Choice(['table', 'json']), help='Output format')
@click.pass_context
def runlogs_latest(ctx, profile_id, output):
    """Get latest execution for profile ID"""
    client, config_manager = get_client_and_config(ctx)
    use_json = should_use_json_output(output, config_manager.get('default_output'))

    try:
        workflow_base = f"https://{client.region}.api.insight.rapid7.com/surface/workflow-api"

        # Get all executions and find the latest for this profile
        response = client.make_request('GET', f"{workflow_base}/executions?size=50")
        if response.status_code != 200:
            raise APIError(f"Failed to get executions: {response.status_code} - {response.text}")

        all_executions = response.json()
        profile_executions = [e for e in all_executions if e.get('samos_workflow_id', '').startswith(f"{profile_id}/")]

        if not profile_executions:
            click.echo(f"No executions found for profile ID: {profile_id}", err=True)
            return

        # Get the latest execution
        latest = sorted(profile_executions, key=lambda x: x.get('timestamp', ''), reverse=True)[0]
        exec_id = latest['execution_id']

        # Get execution logs
        logs_response = client.make_request('GET',
            f"{workflow_base}/executions/{exec_id}/logs?only_user_msgs=true&size=1000&offset=0")

        if logs_response.status_code != 200:
            raise APIError(f"Failed to get execution logs: {logs_response.status_code} - {logs_response.text}")

        logs = logs_response.json()

        if use_json:
            output_data = {
                'execution': latest,
                'logs': logs
            }
            click.echo(json.dumps(output_data, indent=2))
        else:
            console.print(f"[bold cyan]Latest Execution for Profile[/bold cyan]\n")
            console.print(f"Profile ID: {profile_id}")
            console.print(f"Execution ID: {exec_id}")
            console.print(f"Status: {latest.get('status', 'unknown')}")
            console.print(f"Workflow: {latest.get('samos_workflow_id', '')}")
            console.print(f"Timestamp: {latest.get('timestamp', '')}")

            # Calculate duration if we have logs
            if len(logs) >= 2:
                try:
                    from datetime import datetime
                    start_time = datetime.fromisoformat(logs[0]['timestamp'].replace('Z', '+00:00'))
                    end_time = datetime.fromisoformat(logs[-1]['timestamp'].replace('Z', '+00:00'))
                    duration = (end_time - start_time).total_seconds()

                    if duration < 60:
                        duration_str = f"{int(duration)}s"
                    elif duration < 3600:
                        duration_str = f"{int(duration / 60)}m {int(duration % 60)}s"
                    else:
                        duration_str = f"{int(duration / 3600)}h {int((duration % 3600) / 60)}m"

                    console.print(f"Duration: {duration_str}")
                except:
                    pass

            console.print()

            # Display logs
            if logs:
                console.print(f"[bold cyan]Execution Logs[/bold cyan] ({len(logs)} entries)\n")

                for log in logs:
                    timestamp = log.get('timestamp', '')
                    content = log.get('content', {})
                    message = content.get('message', '')
                    level = content.get('level', 'INFO')

                    # Format timestamp
                    try:
                        from datetime import datetime
                        dt = datetime.fromisoformat(timestamp.replace('Z', '+00:00'))
                        time_str = dt.strftime('%H:%M:%S')
                    except:
                        time_str = timestamp[:8] if timestamp else ''

                    # Color by level
                    if level == 'ERROR':
                        console.print(f"[red]{time_str}[/red] {message}")
                    elif level == 'WARNING':
                        console.print(f"[yellow]{time_str}[/yellow] {message}")
                    else:
                        console.print(f"[dim]{time_str}[/dim] {message}")
            else:
                console.print("No logs found", style="yellow")

    except (APIError, ConfigurationError) as e:
        click.echo(f"Error: {e}", err=True)

@asm_group.group(name='cypher')
def cypher_group():
    """ASM Cypher query commands"""
    pass

@cypher_group.command(name='query')
@click.argument('query', required=False)
@click.option('--file', '-f', type=click.Path(exists=True), help='Read query from file (.cypher or .cql)')
@click.option('--columns', default='[]', help='JSON array of columns (e.g., [{"alias":"m","property_name":"name"}])')
@click.option('--output', type=click.Choice(['table', 'json']), help='Output format')
@click.option('--limit', type=int, default=100, show_default=True, help='Max rows to request and display')
@click.option('--start', type=int, default=0, show_default=True, help='Pagination offset - position in result set to start')
@click.option('--depth', type=int, default=0, show_default=True, help='Graph traversal depth for nested relationships')
@click.option('--order/--no-order', default=True, show_default=True, help='Enable/disable result ordering')
@click.option('--use-primary/--no-use-primary', default=False, show_default=True, help='Use primary properties for selection')
@click.option('--no-cache', is_flag=True, help='Disable caching for this query')
@click.pass_context
def cypher_query(ctx, query, file, columns, output, limit, start, depth, order, use_primary, no_cache):
    """Execute ASM Cypher queries"""
    client, config_manager = get_client_and_config(ctx)
    use_json = should_use_json_output(output, config_manager.get('default_output'))
    try:
        # Handle query input - either from argument or file
        if file and query:
            raise click.BadParameter("Cannot specify both query argument and --file option")
        elif file:
            try:
                with open(file, 'r', encoding='utf-8') as f:
                    query = f.read().strip()
                # Remove comments and clean up multiline query
                lines = []
                for line in query.split('\n'):
                    line = line.strip()
                    if line and not line.startswith('//'):
                        lines.append(line)
                query = ' '.join(lines)
            except Exception as e:
                raise click.BadParameter(f"Failed to read query file: {e}")
        elif not query:
            raise click.BadParameter("Must specify either query argument or --file option")
        
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
                    # No columns specified; try to parse RETURN clause for column names
                    parsed_headers = _parse_return_clause(query)
                    if parsed_headers and len(parsed_headers) == inferred_len:
                        # Use parsed headers if they match the data length
                        col_headers = parsed_headers
                    else:
                        # Fallback to generic headers if parsing failed or length mismatch
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
        click.echo(f"Error: {e}", err=True)

@cypher_group.command(name='docs')
def cypher_docs():
    """Show Cypher DSL reference guide"""
    try:
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
@click.option('--test', is_flag=True, help='Execute each example and report results')
@click.pass_context
def cypher_examples(ctx, output, test):
    """List and test Cypher query examples from files"""
    from pathlib import Path
    
    # Load examples from .cypher files
    try:
        import importlib.resources as resources
        
        # Try to access the examples package
        try:
            examples_package = resources.files('examples.asm')
            examples_files = [f for f in examples_package.iterdir() if f.name.endswith('.cypher')]
        except (ImportError, AttributeError):
            # Fallback for development mode
            examples_dir = Path(__file__).parent.parent / 'examples' / 'asm'
            if not examples_dir.exists():
                click.echo("❌ Examples directory not found: examples/asm/", err=True)
                return
            examples_files = list(examples_dir.glob('*.cypher'))
            
    except Exception as e:
        click.echo(f"❌ Error accessing examples: {e}", err=True)
        return
    
    examples = []
    
    # Read all .cypher files in order
    if 'examples_package' in locals():
        # Using importlib.resources (installed package)
        cypher_files = sorted([f for f in examples_files if f.name.endswith('.cypher')], key=lambda x: x.name)
    else:
        # Using filesystem (development mode)
        cypher_files = sorted(examples_files, key=lambda x: x.name)
    
    for cypher_file in cypher_files:
        try:
            # Handle both importlib.resources and filesystem paths
            if 'examples_package' in locals():
                # Using importlib.resources (installed package)
                content = cypher_file.read_text(encoding='utf-8')
                filename = cypher_file.name
            else:
                # Using filesystem (development mode)
                with open(cypher_file, 'r', encoding='utf-8') as f:
                    content = f.read()
                filename = cypher_file.name
                
            # Parse the file to extract metadata and query
            lines = content.strip().split('\n')
            title = ""
            description = ""
            columns = []
            query_lines = []
            
            comment_count = 0
            for line in lines:
                if line.startswith('//'):
                    comment = line[2:].strip()
                    if comment:
                        if not title:
                            title = comment
                            comment_count += 1
                        elif comment_count == 1:
                            description = comment
                            comment_count += 1
                        elif comment.startswith('Columns:'):
                            # Extract columns from comment
                            cols_str = comment[8:].strip()
                            if cols_str and cols_str != '[]':
                                try:
                                    columns = json.loads(cols_str)
                                except:
                                    columns = []
                elif line.strip() and not line.startswith('//'):
                    query_lines.append(line.strip())
            
            # Join query lines
            query = ' '.join(query_lines)
            
            if query:
                # Get stem from filename (remove .cypher extension)
                file_stem = filename.replace('.cypher', '')
                examples.append({
                    "title": title or file_stem.replace('_', ' ').title(),
                    "description": description,
                    "query": query,
                    "columns": columns,
                    "filename": filename
                })
        except Exception as e:
            file_display = getattr(cypher_file, 'name', str(cypher_file))
            click.echo(f"⚠️  Failed to read {file_display}: {e}", err=True)
    
    if not examples:
        click.echo("No examples found", err=True)
        return
    
    # Test mode
    if test:
        client, _ = get_client_and_config(ctx)
        base_url = client.get_base_url('asm')
        
        results = []
        with Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            TimeElapsedColumn(),
        ) as progress:
            task = progress.add_task("Testing examples...", total=len(examples))
            
            for e in examples:
                url = f"{base_url}?format=json&limit=10"
                body = {"columns": e["columns"], "cypher": e["query"]}
                try:
                    response = client.make_request("POST", url, data=body)
                    if response.status_code != 200:
                        results.append({
                            "title": e['title'], 
                            "status": f"HTTP {response.status_code}",
                            "filename": e['filename']
                        })
                    else:
                        data = response.json()
                        items = len(data.get('items', [])) if isinstance(data, dict) else 0
                        results.append({
                            "title": e['title'], 
                            "status": "✓" if items > 0 else "empty",
                            "filename": e['filename']
                        })
                except Exception as ex:
                    results.append({
                        "title": e['title'], 
                        "status": f"error: {ex}",
                        "filename": e['filename']
                    })
                progress.advance(task)
        
        # Display test results
        if output == 'json':
            click.echo(json.dumps(results, indent=2))
        else:
            table = Table(title="Cypher Examples Test Results")
            table.add_column("File", style="cyan")
            table.add_column("Title", style="white")
            table.add_column("Status", style="green")
            
            for r in results:
                status_style = "green" if r['status'] == "✓" else "yellow" if r['status'] == "empty" else "red"
                table.add_row(r['filename'], r['title'], f"[{status_style}]{r['status']}[/{status_style}]")
            console.print(table)
        return
    
    # Display mode
    if output == 'json':
        click.echo(json.dumps(examples, indent=2))
    elif output == 'cmd':
        for e in examples:
            if e['columns']:
                click.echo(f"r7 asm cypher query -f examples/asm/{e['filename']} --columns '{json.dumps(e['columns'])}'")
            else:
                click.echo(f"r7 asm cypher query -f examples/asm/{e['filename']}")
    elif output == 'table':
        table = Table(title="ASM Cypher Examples")
        table.add_column("File", style="cyan")
        table.add_column("Title", style="white")
        table.add_column("Description", style="yellow")
        
        for e in examples:
            table.add_row(e['filename'], e['title'], e['description'] or "")
        console.print(table)
    else:  # plain
        for i, e in enumerate(examples, 1):
            click.echo(f"{i}. {e['title']}")
            if e['description']:
                click.echo(f"   {e['description']}")
            click.echo("")
            
            # Show direct query command if short enough, otherwise use file reference
            if len(e['query']) < 150:  # Short queries can be shown inline
                if e['columns']:
                    click.echo(f"   r7 asm cypher query \"{e['query']}\" --columns '{json.dumps(e['columns'])}'")
                else:
                    click.echo(f"   r7 asm cypher query \"{e['query']}\"")
            else:  # Long queries use file reference
                if e['columns']:
                    click.echo(f"   r7 asm cypher query -f examples/asm/{e['filename']} --columns '{json.dumps(e['columns'])}'")
                else:
                    click.echo(f"   r7 asm cypher query -f examples/asm/{e['filename']}")
            click.echo()

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