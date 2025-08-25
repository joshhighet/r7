import json
import sys
import re
from datetime import datetime, timedelta
import click
from urllib.parse import quote
from tabulate import tabulate
from rich.console import Console
from rich.table import Table
from rich.progress import Progress, SpinnerColumn, TextColumn, TimeElapsedColumn
from api.client import Rapid7Client
from utils.config import ConfigManager
from utils.cache import CacheManager
from utils.credentials import CredentialManager
from utils.exceptions import *

console = Console()

def _flatten_group_statistics(groups_list):
    """
    Flatten the statistics.groups structure from LEQL into rows for table display.
    Supports single or nested groupby without being log-type specific.
    Returns a tuple of (rows, metric_keys).
    Each row is a dict: {"group": "path", <metric>: value, ...}
    """
    rows = []
    metric_keys_all = set()

    def is_metrics_dict(d):
        # A metrics dict contains numeric values (e.g., {"count": 118.0, ...})
        return isinstance(d, dict) and d and all(isinstance(v, (int, float)) for v in d.values())

    def rec(node, path):
        if is_metrics_dict(node):
            rows.append({"group": " / ".join(map(str, path)), **node})
            metric_keys_all.update(node.keys())
            return
        if isinstance(node, dict):
            # Some structures put metrics under a 'totals' dict
            if "totals" in node and is_metrics_dict(node["totals"]):
                totals = node["totals"]
                rows.append({"group": " / ".join(map(str, path)), **totals})
                metric_keys_all.update(totals.keys())
                return
            # Otherwise recurse into children (nested groups)
            for k, v in node.items():
                rec(v, path + [k])

    for entry in groups_list or []:
        if isinstance(entry, dict):
            for k, v in entry.items():
                # If v is directly a metrics dict, use k as the group name
                if is_metrics_dict(v):
                    # Clean up the group key format [a, b] -> a | b
                    clean_key = k
                    if k.startswith('[') and k.endswith(']'):
                        inner = k[1:-1]
                        parts = [part.strip() for part in inner.split(',')]
                        clean_key = ' | '.join(parts)
                    rows.append({"group": clean_key, **v})
                    metric_keys_all.update(v.keys())
                else:
                    rec(v, [k])

    return rows, sorted(metric_keys_all)

def _render_groupby_table(statistics, title_suffix=""):
    """
    Render a generic table for statistics with groupby results.
    """
    groups = statistics.get("groups") or []
    if not groups:
        return False  # nothing to render here

    rows, metric_keys = _flatten_group_statistics(groups)
    if not rows:
        return False

    table = Table(title=f"Group Results{f' - {title_suffix}' if title_suffix else ''}")
    table.add_column("Group", style="cyan")
    for mk in metric_keys or ["count"]:
        table.add_column(mk.capitalize(), style="yellow", justify="right")

    # Sort rows by primary metric if available
    primary_metric = metric_keys[0] if metric_keys else "count"
    try:
        rows = sorted(rows, key=lambda r: r.get(primary_metric, 0), reverse=True)
    except Exception:
        pass

    for r in rows[:100]:  # cap rows for readability
        values = [r.get(mk, 0) for mk in metric_keys or ["count"]]
        table.add_row(r.get("group", ""), *[f"{v:.0f}" if isinstance(v, float) else str(v) for v in values])

    console.print(table)

    if len(rows) > 100:
        console.print(f"[dim]... and {len(rows) - 100} more groups (use --output json to see all)[/dim]")
    return True

def parse_leql_limit(query):
    """
    Parse LEQL query to extract limit value if present.
    Returns the limit as an integer, or None if no limit found.
    """
    if not query:
        return None
    
    # Look for limit(N) pattern, case insensitive
    limit_match = re.search(r'limit\s*\(\s*(\d+)\s*\)', query, re.IGNORECASE)
    if limit_match:
        return int(limit_match.group(1))
    
    return None

def calculate_smart_max_pages(query, default_max_pages):
    """
    Calculate smart max pages based on LEQL query content.
    If query has a limit() clause, restrict pages to avoid over-fetching.
    """
    limit = parse_leql_limit(query)
    if limit is not None:
        # Estimate pages needed: assume ~5 events per page on average
        # Add 1 page buffer but cap at reasonable limit
        estimated_pages = max(1, min(3, (limit // 5) + 1))
        return estimated_pages
    
    return default_max_pages

def handle_smart_pagination(query, max_result_pages, config_manager, use_json):
    """
    Handle smart pagination logic for queries.
    Returns the calculated max_result_pages and prints info if needed.
    """
    if max_result_pages is None:
        default_max = config_manager.get('max_result_pages', 10)
        max_result_pages = calculate_smart_max_pages(query, default_max)
        
        # Show info about smart pagination if limit detected
        limit = parse_leql_limit(query)
        if limit is not None and not use_json:
            console.print(f"[dim]📊 Detected LEQL limit({limit}), using {max_result_pages} pages max[/dim]")
    
    return max_result_pages

def display_raw_events_table(events, title, max_events=50, extra_columns=None, max_chars=500):
    """
    Display events in a table with raw log content.
    extra_columns: list of tuples (column_name, getter_function) for additional columns
    max_chars: maximum characters to display per log entry
    """
    table = Table(title=title)
    
    # Add extra columns if specified (e.g., for multi-logset queries)
    if extra_columns:
        for col_name, col_style, col_width in extra_columns:
            table.add_column(col_name, style=col_style, width=col_width)
    
    table.add_column("Time", style="cyan", width=20)
    table.add_column("Raw Log", style="white", width=150 if not extra_columns else 120)
    
    events_to_show = events[:max_events]
    
    for event in events_to_show:
        row_data = []
        
        # Add extra column values if specified
        if extra_columns:
            for _, _, _, getter in extra_columns:
                row_data.append(getter(event))
        
        # Add time and raw log
        row_data.append(format_timestamp(event.get('timestamp')))
        content = event.get('message', '')
        if not content:
            content = str(event)
        
        # Apply character limiting for raw log content
        if len(content) > max_chars:
            content = content[:max_chars] + "..."
        
        row_data.append(content)
        
        table.add_row(*row_data)
    
    console.print(table)
    
    # Show if there are more events
    total_events = len(events)
    if total_events > max_events:
        console.print(f"[dim]... and {total_events - max_events} more events (use --output json to see all)[/dim]")
    
    return total_events

def format_timestamp(timestamp):
    """Convert unix timestamp (milliseconds) to readable format"""
    if not timestamp:
        return ''
    try:
        # Convert milliseconds to seconds
        dt = datetime.fromtimestamp(timestamp / 1000)
        return dt.strftime('%Y-%m-%d %H:%M:%S')
    except (ValueError, TypeError):
        return str(timestamp)


def get_topkeys_for_log(client, log_id):
    """Fetch topkeys data for a log, with caching"""
    try:
        base_url = client.get_base_url('idr')
        cache_key = f"topkeys_{log_id}"
        
        # Check cache first
        cached_result = None
        if client.cache_manager:
            cached_result = client.cache_manager.get('topkeys', cache_key)
        
        if cached_result:
            return cached_result.get('topkeys', [])
        
        # Fetch from API
        url = f"{base_url}/management/logs/{log_id}/topkeys"
        response = client.make_request("GET", url)
        
        if response.status_code == 200:
            data = response.json()
            # Cache the result
            if client.cache_manager:
                client.cache_manager.set('topkeys', cache_key, data)
            return data.get('topkeys', [])
    except Exception:
        # If topkeys fails, return empty list
        pass
    
    return []


def extract_smart_field_value(field_name, parsed_data, event):
    """
    Intelligently extract a useful value from a field, handling nested objects.
    Returns (display_name, clean_value) or None if no useful value found.
    """
    value = None
    
    if field_name.startswith('json.'):
        # Navigate nested JSON structure
        field_path = field_name[5:].split('.')
        value = parsed_data
        for path_part in field_path:
            if isinstance(value, dict) and path_part in value:
                value = value[path_part]
            else:
                value = None
                break
    else:
        # Direct field access
        value = event.get(field_name)
    
    # If we got an object/dict, try to extract useful sub-fields
    if isinstance(value, dict):
        # Look for useful leaf values within the object
        useful_pairs = []
        
        def extract_useful_from_dict(obj, prefix="", max_depth=3, current_depth=0):
            if current_depth >= max_depth or not isinstance(obj, dict):
                return
                
            for key, val in obj.items():
                current_path = f"{prefix}.{key}" if prefix else key
                
                # Prefer primitive values (strings, numbers, booleans)
                if isinstance(val, (str, int, float, bool)) and str(val).strip():
                    # Skip very long values or ones that look like IDs/GUIDs
                    val_str = str(val)
                    if len(val_str) <= 100 and not (len(val_str) > 20 and all(c in '0123456789abcdefABCDEF-{}' for c in val_str)):
                        useful_pairs.append((key, val_str))
                        if len(useful_pairs) >= 3:  # Limit to first 3 useful fields
                            return
                elif isinstance(val, dict):
                    # Recurse into nested objects
                    extract_useful_from_dict(val, current_path, max_depth, current_depth + 1)
                elif isinstance(val, list) and val and len(val) <= 5:
                    # Handle small arrays
                    if all(isinstance(item, (str, int, float)) for item in val):
                        useful_pairs.append((key, ', '.join(str(item) for item in val[:3])))
                        if len(useful_pairs) >= 3:
                            return
        
        extract_useful_from_dict(value)
        
        # If we found useful sub-fields, combine them
        if useful_pairs:
            # Use the first useful field for this column
            display_name = useful_pairs[0][0].replace('_', ' ').title()
            clean_value = useful_pairs[0][1][:30]
            if len(useful_pairs[0][1]) > 30:
                clean_value += "..."
            return display_name, clean_value
        
        # If no useful sub-fields, return None to skip this column
        return None
    
    # Handle primitive values
    elif value is not None and str(value).strip():
        # Create clean display name from field path
        display_name = field_name.split('.')[-1].replace('_', ' ').title()
        clean_value = str(value)[:30]
        if len(str(value)) > 30:
            clean_value += "..."
        return display_name, clean_value
    
    return None

def get_smart_column_definitions(topkeys_data, parsed_data, event, max_cols=6):
    """
    Generate smart column definitions by analyzing topkeys and extracting useful fields.
    Returns list of (field_name, display_name) tuples.
    """
    column_defs = []
    used_display_names = set()
    
    # Common time-related field patterns to skip (since we already have a Time column)
    time_field_patterns = [
        'time', 'timestamp', 'date', 'datetime', 'created', 'updated', 
        'start_time', 'end_time', 'event_time', 'log_time', 'ingestion_time',
        'start time', 'end time', 'event time', 'log time', 'ingestion time'
    ]
    
    # Sort topkeys by weight
    top_fields = sorted(topkeys_data, key=lambda x: x.get('weight', 0), reverse=True)
    
    # Process more fields than needed to account for filtering
    fields_checked = 0
    max_checks = min(len(top_fields), max_cols * 3)  # Check up to 3x max_cols to find enough non-time fields
    
    for field_info in top_fields:
        if len(column_defs) >= max_cols:
            break
        
        fields_checked += 1
        if fields_checked > max_checks:
            break
            
        field_name = field_info.get('key', '')
        if not field_name:
            continue
        
        # Try to extract a useful value from this field
        result = extract_smart_field_value(field_name, parsed_data, event)
        if result:
            display_name, _ = result
            
            # Skip if we already have a column with this display name
            if display_name in used_display_names:
                continue
            
            # Skip time-related fields since we already have a Time column
            display_name_lower = display_name.lower()
            if any(pattern in display_name_lower for pattern in time_field_patterns):
                continue
                
            column_defs.append((field_name, display_name))
            used_display_names.add(display_name)
    
    return column_defs

def should_use_json_output(output_format, config_default):
    """Determine if we should use JSON output based on pipe detection and user preference"""
    if output_format:
        return output_format == 'json'
    if not sys.stdout.isatty():
        return True
    return config_default == 'json'

def format_bytes(bytes_value):
    """Format bytes into human readable format"""
    if not bytes_value:
        return "0 B"
    
    for unit in ['B', 'KB', 'MB', 'GB', 'TB', 'PB']:
        if bytes_value < 1024.0:
            return f"{bytes_value:.1f} {unit}"
        bytes_value /= 1024.0
    return f"{bytes_value:.1f} EB"

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

# This group will be added to siem_group in idr_commands.py
@click.group(name='logs')
def siem_logs_group():
    """Commands for SIEM logs"""
    pass

@siem_logs_group.command(name='leql')
def logs_leql():
    """Show LEQL (Log Entry Query Language) reference guide"""
    try:
        import os
        from pathlib import Path
        
        # Get the path to the LEQL reference file
        current_dir = Path(__file__).parent.parent
        leql_file = current_dir / 'docs/leql-dsl.md'
        
        if not leql_file.exists():
            click.echo("❌ LEQL reference file not found: leql-dsl.md", err=True)
            return
            
        # Read and display the markdown file
        with open(leql_file, 'r', encoding='utf-8') as f:
            content = f.read()
            
        # Use rich console for better formatting if available
        try:
            from rich.console import Console
            from rich.markdown import Markdown
            
            console = Console()
            md = Markdown(content)
            console.print(md)
        except ImportError:
            # Fallback to plain text
            click.echo(content)
            
    except Exception as e:
        click.echo(f"❌ Error displaying LEQL help: {e}", err=True)


@siem_logs_group.command(name='query')
@click.argument('log_name_or_id')
@click.argument('query', required=False, default='')
@click.option('--time-range', default='Last 30 days', help='Time range')
@click.option('--output', type=click.Choice(['table', 'json']), help='Output format')
@click.option('--full-output', is_flag=True, help='Show complete JSON structure (default shows only events)')
@click.option('--max-result-pages', type=int, help='Max result pages to fetch (overrides smart limit detection)')
@click.option('--no-cache', is_flag=True, help='Disable caching for this query')
@click.option('--no-smart-columns', is_flag=True, help='Disable smart columns and use simple content display')
@click.option('--smart-columns-max', type=int, help='Maximum number of smart columns to display')
@click.option('--max-chars', type=int, help='Maximum characters to display per log entry (overrides global config)')
@click.pass_context
def query_log(ctx, log_name_or_id, query, time_range, output, full_output, max_result_pages, no_cache, no_smart_columns, smart_columns_max, max_chars):
    """Query InsightIDR log with LEQL"""
    client, config_manager = get_client_and_config(ctx)
    use_json = should_use_json_output(output, config_manager.get('default_output'))
    
    # Smart pagination
    max_result_pages = handle_smart_pagination(query, max_result_pages, config_manager, use_json)
    
    # Determine max characters for log content display
    if max_chars is not None:
        effective_max_chars = max_chars
    else:
        effective_max_chars = config_manager.get('max_chars', 500)
    
    try:
        base_url = client.get_base_url('idr')
        if not client.is_uuid(log_name_or_id):
            log_id = client.get_log_id_by_name(log_name_or_id)
        else:
            log_id = log_name_or_id
        cache_key = f"{log_id}_{query}_{time_range}_{max_result_pages}"
        cached_result = None
        if client.cache_manager and not no_cache:
            cached_result = client.cache_manager.get('leql_query', cache_key)
            if cached_result:
                if not use_json:
                    console.print("📋 Using cached result", style="dim")
        if not cached_result:
            query_base_url = client.get_base_url('idr_query')
            encoded_query = quote(query)
            encoded_time = quote(time_range)
            url = f"{query_base_url}/query/logs/{log_id}?time_range={encoded_time}&query={encoded_query}"
            data = client.poll_query(url, show_progress=not use_json, max_result_pages=max_result_pages, query_timeout=config_manager.get('query_timeout'))
            if client.cache_manager and not no_cache:
                client.cache_manager.set('leql_query', cache_key, data)
        else:
            data = cached_result
        if use_json:
            if full_output:
                click.echo(json.dumps(data, indent=2))
            else:
                # Show only raw log messages by default
                events = data.get('events', [])
                messages = []
                for event in events:
                    message = event.get('message', '')
                    # Try to parse JSON message, fallback to raw string if not JSON
                    try:
                        parsed_message = json.loads(message)
                        messages.append(parsed_message)
                    except (json.JSONDecodeError, TypeError):
                        messages.append(message)
                click.echo(json.dumps(messages))
        else:
            # Check if we have events OR statistics
            has_events = 'events' in data and data['events']
            has_stats = 'statistics' in data and data['statistics']
            
            if has_events:
                # Determine if smart columns should be used (default enabled, can be disabled)
                use_smart_columns = not no_smart_columns and config_manager.get('smart_columns_enabled', True)
                
                # Get smart columns max from config or parameter
                if smart_columns_max is not None:
                    max_cols = smart_columns_max
                else:
                    max_cols = config_manager.get('smart_columns_max', 4)
                
                # Get topkeys data if smart-columns is enabled
                topkeys_data = []
                if use_smart_columns:
                    topkeys_data = get_topkeys_for_log(client, log_id)
                    if topkeys_data and not use_json:
                        max_cols = max(1, min(10, max_cols))
                        actual_cols = min(len(topkeys_data), max_cols)
                        console.print(f"[dim]🧠 Using {len(topkeys_data)} topkeys, creating {actual_cols} smart columns[/dim]")
                
                table = Table(title=f"Query Results: {log_name_or_id}")
                table.add_column("Time", style="cyan", width=20)
                
                events_to_show = data['events'][:50]  # Still limit display to 50 for readability
                
                # Create dynamic columns based on topkeys
                if use_smart_columns and topkeys_data:
                    # Get first event to analyze structure for column definitions
                    sample_event = events_to_show[0] if events_to_show else {}
                    sample_message = sample_event.get('message', '')
                    sample_parsed = {}
                    if sample_message and sample_message.startswith('{') and sample_message.endswith('}'):
                        try:
                            sample_parsed = json.loads(sample_message)
                        except json.JSONDecodeError:
                            pass
                    
                    # Get intelligent column definitions
                    max_cols = max(1, min(10, max_cols))  # Enforce reasonable bounds
                    column_defs = get_smart_column_definitions(topkeys_data, sample_parsed, sample_event, max_cols)
                    
                    # Add columns based on intelligent analysis
                    column_fields = []
                    for field_name, display_name in column_defs:
                        table.add_column(display_name, style="white", width=25)
                        column_fields.append(field_name)
                    
                    # If no useful columns found, fall back to content column
                    if not column_fields:
                        table.add_column("Content", style="white", width=100)
                else:
                    # Raw log column - make it wider for full log display
                    table.add_column("Raw Log", style="white", width=effective_max_chars, overflow="fold")
                    column_fields = []
                
                for event in events_to_show:
                    # Parse the message JSON if available
                    message = event.get('message', '')
                    parsed_data = {}
                    if message and message.startswith('{') and message.endswith('}'):
                        try:
                            parsed_data = json.loads(message)
                        except json.JSONDecodeError:
                            pass
                    
                    row_data = [format_timestamp(event.get('timestamp'))]
                    
                    if use_smart_columns and column_fields:
                        # Extract values for each column using intelligent extraction
                        for field_name in column_fields:
                            result = extract_smart_field_value(field_name, parsed_data, event)
                            if result:
                                _, clean_value = result
                                row_data.append(clean_value)
                            else:
                                row_data.append("-")
                    else:
                        # Show raw message when smart columns are disabled
                        content = event.get('message', '')
                        if not content:
                            # If no message field, show whatever is available
                            content = str(event)
                        
                        # Apply character limiting for raw log content
                        if len(content) > effective_max_chars:
                            content = content[:effective_max_chars] + "..."
                        
                        row_data.append(content)
                    
                    table.add_row(*row_data)
                console.print(table)
                
                total_events = len(data['events'])
                if total_events > 50:
                    console.print(f"[dim]... and {total_events - 50} more events (use --output json to see all)[/dim]")
                
                # Show pagination info
                if total_events > 0:
                    console.print(f"[dim]📄 Retrieved {total_events} events across {max_result_pages} pages[/dim]")
                    
            elif has_stats:
                statistics = data.get('statistics', {})
                # Prefer groupby rendering when available
                rendered = _render_groupby_table(statistics, title_suffix=str(log_name_or_id))
                if not rendered:
                    stats = statistics.get('stats', {}).get('global_timeseries', {})
                    if stats:
                        table = Table(title="Query Statistics")
                        table.add_column("Stat", style="cyan")
                        table.add_column("Value", style="yellow")
                        for k, v in stats.items():
                            table.add_row(k, str(v))
                        console.print(table)
                    else:
                        console.print("No statistics data to display", style="yellow")
            else:
                console.print("No results found", style="yellow")
    except (APIError, QueryError) as e:
        click.echo(f"❌ {e}", err=True)


@siem_logs_group.command(name='query-logset')
@click.argument('logset_name_or_id')
@click.argument('query', required=False, default='')
@click.option('--time-range', default='Last 30 days', help='Time range')
@click.option('--output', type=click.Choice(['table', 'json']), help='Output format')
@click.option('--full-output', is_flag=True, help='Show complete JSON structure (default shows only events)')
@click.option('--max-result-pages', type=int, help='Max result pages to fetch (overrides smart limit detection)')
@click.option('--no-cache', is_flag=True, help='Disable caching for this query')
@click.pass_context
def query_logset(ctx, logset_name_or_id, query, time_range, output, full_output, max_result_pages, no_cache):
    """Query an entire logset with LEQL"""
    client, config_manager = get_client_and_config(ctx)
    use_json = should_use_json_output(output, config_manager.get('default_output'))
    
    # Smart pagination
    max_result_pages = handle_smart_pagination(query, max_result_pages, config_manager, use_json)
    
    try:
        cache_key = f"logset_{logset_name_or_id}_{query}_{time_range}_{max_result_pages}"
        cached_result = None
        if client.cache_manager and not no_cache:
            cached_result = client.cache_manager.get('leql_query', cache_key)
            if cached_result:
                if not use_json:
                    console.print("📋 Using cached result", style="dim")
        
        if not cached_result:
            data = client.query_logset(logset_name_or_id, query, time_range, max_result_pages)
            if client.cache_manager and not no_cache:
                client.cache_manager.set('leql_query', cache_key, data)
        else:
            data = cached_result
            
        if use_json:
            if full_output:
                click.echo(json.dumps(data, indent=2))
            else:
                # Show only raw log messages by default
                events = data.get('events', [])
                messages = []
                for event in events:
                    message = event.get('message', '')
                    # Try to parse JSON message, fallback to raw string if not JSON
                    try:
                        parsed_message = json.loads(message)
                        messages.append(parsed_message)
                    except (json.JSONDecodeError, TypeError):
                        messages.append(message)
                click.echo(json.dumps(messages))
        else:
            # Check if we have events OR statistics
            has_events = 'events' in data and data['events']
            has_stats = 'statistics' in data and data['statistics']
            
            if has_events:
                total_events = display_raw_events_table(
                    data['events'],
                    f"Logset Query Results: {logset_name_or_id}"
                )
                
                # Show pagination info
                if total_events > 0:
                    console.print(f"[dim]📄 Retrieved {total_events} events across {max_result_pages} pages[/dim]")
                    
            elif has_stats:
                statistics = data.get('statistics', {})
                # Prefer groupby rendering when available
                rendered = _render_groupby_table(statistics, title_suffix=str(logset_name_or_id))
                if not rendered:
                    stats = statistics.get('stats', {}).get('global_timeseries', {})
                    if stats:
                        table = Table(title="Query Statistics")
                        table.add_column("Stat", style="cyan")
                        table.add_column("Value", style="yellow")
                        for k, v in stats.items():
                            table.add_row(k, str(v))
                        console.print(table)
                    else:
                        console.print("No statistics data to display", style="yellow")
            else:
                console.print("No results found", style="yellow")
    except (APIError, QueryError) as e:
        click.echo(f"❌ {e}", err=True)


@siem_logs_group.command(name='query-all')
@click.argument('query', required=False, default='')
@click.option('--time-range', default='Last 30 days', help='Time range')
@click.option('--output', type=click.Choice(['table', 'json']), help='Output format')
@click.option('--full-output', is_flag=True, help='Show complete JSON structure (default shows only events)')
@click.option('--max-result-pages', type=int, help='Max result pages to fetch (overrides smart limit detection)')
@click.option('--no-cache', is_flag=True, help='Disable caching for this query')
@click.pass_context
def query_all_logsets(ctx, query, time_range, output, full_output, max_result_pages, no_cache):
    """Query all logsets at once with LEQL"""
    client, config_manager = get_client_and_config(ctx)
    use_json = should_use_json_output(output, config_manager.get('default_output'))
    
    # Smart pagination
    max_result_pages = handle_smart_pagination(query, max_result_pages, config_manager, use_json)
    
    try:
        # First, get info about available logsets for visibility
        if not use_json:
            console.print("[dim]🔍 Querying all logsets in your organization...[/dim]")
            
            # Show logset count for transparency
            try:
                base_url = client.get_base_url('idr')
                url = f"{base_url}/management/logs"
                response = client.make_request("GET", url)
                if response.status_code == 200:
                    logs_data = response.json()['logs']
                    logset_names = set()
                    for log in logs_data:
                        for logset_info in log.get('logsets_info', []):
                            logset_names.add(logset_info.get('name', 'Unknown'))
                    console.print(f"[dim]📊 Found {len(logset_names)} unique logsets to query[/dim]")
            except Exception:
                pass  # Don't fail the query if we can't get logset info
        
        cache_key = f"all_logsets_{query}_{time_range}_{max_result_pages}"
        cached_result = None
        if client.cache_manager and not no_cache:
            cached_result = client.cache_manager.get('leql_query', cache_key)
            if cached_result:
                if not use_json:
                    console.print("📋 Using cached result", style="dim")
        
        if not cached_result:
            data = client.query_all_logsets(query, time_range, max_result_pages)
            if client.cache_manager and not no_cache:
                client.cache_manager.set('leql_query', cache_key, data)
        else:
            data = cached_result
            
        if use_json:
            if full_output:
                click.echo(json.dumps(data, indent=2))
            else:
                # Show only raw log messages by default
                events = data.get('events', [])
                messages = []
                for event in events:
                    message = event.get('message', '')
                    # Try to parse JSON message, fallback to raw string if not JSON
                    try:
                        parsed_message = json.loads(message)
                        messages.append(parsed_message)
                    except (json.JSONDecodeError, TypeError):
                        messages.append(message)
                click.echo(json.dumps(messages))
        else:
            # Check if we have events OR statistics
            has_events = 'events' in data and data['events']
            has_stats = 'statistics' in data and data['statistics']
            
            if has_events:
                total_events = display_raw_events_table(
                    data['events'],
                    "All Logsets Query Results"
                )
                
                # Show pagination info
                if total_events > 0:
                    console.print(f"[dim]📄 Retrieved {total_events} events across all logsets ({max_result_pages} pages max)[/dim]")
                    
            elif has_stats:
                statistics = data.get('statistics', {})
                # Prefer groupby rendering when available
                rendered = _render_groupby_table(statistics, title_suffix="All Logsets")
                if not rendered:
                    stats = statistics.get('stats', {}).get('global_timeseries', {})
                    if stats:
                        table = Table(title="Query Statistics")
                        table.add_column("Stat", style="cyan")
                        table.add_column("Value", style="yellow")
                        for k, v in stats.items():
                            table.add_row(k, str(v))
                        console.print(table)
                    else:
                        console.print("No statistics data to display", style="yellow")
            else:
                console.print("No results found", style="yellow")
    except (APIError, QueryError) as e:
        click.echo(f"❌ {e}", err=True)

# Enhance examples to show full command lines with log id
@siem_logs_group.command(name='examples')
@click.option('--output', type=click.Choice(['table', 'json', 'plain']), default='plain', help='How to display the examples')
@click.option('--log-id', help='A concrete log UUID to embed into the example command lines')
def logs_examples(output, log_id):
    """Show curated InsightIDR LEQL examples with full command lines requiring a log id."""
    lid = log_id or '<LOG_ID>'
    examples = [
        {
            "title": "Group InsightIDR alerts by status",
            "query": "groupby(\"service_info.status\")",
            "time_range": "Last 24 hours",
            "cmd": f"r7 siem logs query \"InsightIDR Alerts\" \"groupby(\\\"service_info.status\\\")\" --time-range \"Last 24 hours\"",
            "notes": "Group and analyze alert statuses from InsightIDR."
        },
        {
            "title": "Group job status by hostname and status",
            "query": "groupby(\"hostname\",\"status\")",
            "time_range": "Last 7 days",
            "cmd": f"r7 siem logs query \"Job Status\" \"groupby(\\\"hostname\\\",\\\"status\\\")\" --time-range \"Last 7 days\"",
            "notes": "Analyze job statuses across different hostnames."
        },
        {
            "title": "Analyze flagged messages from Sublime Security",
            "query": "where(\"type\"=\"message.flagged\" AND \"org_id\"=\"1e3aed28-2bdc-44f4-ae68-c2953ad94a12\") groupby(\"data.flagged_rules.0.detection_methods.2\",\"data.flagged_rules.0.tactics_and_techniques.0\")",
            "time_range": "Last 24 hours",
            "cmd": f"r7 siem logs query \"sublime-security\" \"where(\\\"type\\\"=\\\"message.flagged\\\" AND \\\"org_id\\\"=\\\"1e3aed28-2bdc-44f4-ae68-c2953ad94a12\\\") groupby(\\\"data.flagged_rules.0.detection_methods.2\\\",\\\"data.flagged_rules.0.tactics_and_techniques.0\\\")\" --time-range \"Last 24 hours\"",
            "notes": "Filter and group flagged messages by detection methods and tactics."
        },
        {
            "title": "Select geographic and account information",
            "query": "select(\"geoip_country_code\",\"geoip_organization\",\"account\",\"result\", \"source_json.event.parameters.0.value\",\"source_json.event.parameters.1.multiValue.0\")",
            "time_range": "Last 4 hours",
            "cmd": f"r7 siem logs query c33297ce-3878-46a4-a0e8-2d62116ed541 \"select(\\\"geoip_country_code\\\",\\\"geoip_organization\\\",\\\"account\\\",\\\"result\\\", \\\"source_json.event.parameters.0.value\\\",\\\"source_json.event.parameters.1.multiValue.0\\\")\" --time-range \"Last 4 hours\"",
            "notes": "Extract specific geographic and account fields from log events."
        }
    ]

    if output == 'json':
        click.echo(json.dumps(examples, indent=2))
        return
    if output == 'plain':
        for i, e in enumerate(examples, 1):
            click.echo(f"{i}. {e['title']}")
            click.echo(f"   {e['notes']}")
            click.echo(f"")
            click.echo(f"   {e['cmd']}")
            click.echo()
        return
    table = Table(title="InsightIDR LEQL Examples (with full commands)")
    table.add_column("Title", style="cyan")
    table.add_column("LEQL", style="white")
    table.add_column("Time Range", style="magenta")
    table.add_column("Command", style="green")
    table.add_column("Notes", style="yellow")
    for e in examples:
        table.add_row(e['title'], e['query'], e['time_range'], e['cmd'], e['notes'])
    console.print(table)



@siem_logs_group.command(name='usage-specific')
@click.argument('log_key')
@click.option('--from-date', help='Start date (YYYY-MM-DD format)')
@click.option('--to-date', help='End date (YYYY-MM-DD format)')
@click.option('--output', type=click.Choice(['table', 'json']), default='table', help='Output format')
@click.option('--no-cache', is_flag=True, help='Disable caching for this query')
@click.pass_context
def usage_specific(ctx, log_key, from_date, to_date, output, no_cache):
    """Show usage for a specific log with detailed daily breakdown and analytics"""
    client, config_manager = get_client_and_config(ctx)
    use_json = (output == 'json')
    
    try:
        # Validate that both dates are provided (required for this endpoint)
        if not from_date or not to_date:
            # Default to last 7 days for more useful data
            end_date = datetime.now()
            start_date = end_date - timedelta(days=7)
            from_date = start_date.strftime('%Y-%m-%d')
            to_date = end_date.strftime('%Y-%m-%d')

        # Try to resolve log name from log_key if it's a UUID
        log_name = log_key
        try:
            if client.is_uuid(log_key):
                # Get list of logs to find the name
                base_url = client.get_base_url('idr')
                logs_url = f"{base_url}/management/logs"
                logs_response = client.make_request("GET", logs_url)
                if logs_response.status_code == 200:
                    logs_data = logs_response.json().get('logs', [])
                    for log in logs_data:
                        if log.get('id') == log_key:
                            log_name = log.get('name', log_key)
                            break
        except Exception:
            # If name resolution fails, just use the key
            pass

        # Get specific log usage data
        cache_key = f"specific_log_usage_{log_key}_{from_date}_{to_date}"
        cached_data = None
        if client.cache_manager and not no_cache:
            cached_data = client.cache_manager.get('log_usage', cache_key)
            if cached_data and not use_json:
                console.print("📋 Using cached result", style="dim")

        if not cached_data:
            data = client.get_specific_log_usage(log_key, from_date, to_date)
            
            if client.cache_manager and not no_cache:
                client.cache_manager.set('log_usage', cache_key, data)
        else:
            data = cached_data

        if use_json:
            click.echo(json.dumps(data, indent=2))
            return

        # Display table format
        usage_info = data.get('usage', {})
        
        if not usage_info:
            console.print(f"No usage data found for log {log_name} in the specified period", style="yellow")
            return

        # Parse the actual data structure from the API
        log_id = usage_info.get('id', log_key)
        period = usage_info.get('period', {})
        daily_usage = usage_info.get('daily_usage', [])
        
        if not daily_usage:
            console.print(f"No daily usage data found for log {log_name}", style="yellow")
            return

        # Calculate statistics
        total_usage = sum(day.get('usage', 0) for day in daily_usage)
        num_days = len(daily_usage)
        avg_daily = total_usage / num_days if num_days > 0 else 0
        max_daily = max((day.get('usage', 0) for day in daily_usage), default=0)
        min_daily = min((day.get('usage', 0) for day in daily_usage), default=0)

        # Main info table
        info_table = Table(title=f"Log Usage Details: {log_name}")
        info_table.add_column("Metric", style="cyan")
        info_table.add_column("Value", style="white")
        
        info_table.add_row("Log Name", log_name)
        info_table.add_row("Log ID", log_id)
        info_table.add_row("Period", f"{period.get('from', from_date)} to {period.get('to', to_date)}")
        info_table.add_row("Total Usage", format_bytes(total_usage))
        info_table.add_row("Daily Average", format_bytes(avg_daily))
        info_table.add_row("Peak Day Usage", format_bytes(max_daily))
        info_table.add_row("Lowest Day Usage", format_bytes(min_daily))
        info_table.add_row("Days with Data", str(num_days))
        
        console.print(info_table)
        
        # Daily breakdown table
        if daily_usage:
            daily_table = Table(title="Daily Usage Breakdown")
            daily_table.add_column("Date", style="cyan")
            daily_table.add_column("Usage", style="white")
            daily_table.add_column("% of Total", style="yellow")
            daily_table.add_column("vs Avg", style="green")
            
            # Sort by date
            sorted_daily = sorted(daily_usage, key=lambda x: x.get('day', ''))
            
            for day_data in sorted_daily:
                day = day_data.get('day', 'Unknown')
                usage = day_data.get('usage', 0)
                
                # Calculate percentage of total
                percentage = (usage / total_usage * 100) if total_usage > 0 else 0
                
                # Compare to average (trend indicator)
                vs_avg = ""
                if usage > avg_daily * 1.2:
                    vs_avg = "📈 +High"
                elif usage < avg_daily * 0.8:
                    vs_avg = "📉 Low"
                else:
                    vs_avg = "➡️ Normal"
                
                daily_table.add_row(
                    day,
                    format_bytes(usage),
                    f"{percentage:.1f}%",
                    vs_avg
                )
            
            console.print(daily_table)
            
            # Show trend summary
            if num_days >= 3:
                recent_days = sorted_daily[-3:]  # Last 3 days
                older_days = sorted_daily[:-3] if len(sorted_daily) > 3 else []
                
                if older_days:
                    recent_avg = sum(day.get('usage', 0) for day in recent_days) / len(recent_days)
                    older_avg = sum(day.get('usage', 0) for day in older_days) / len(older_days)
                    
                    trend = "📈 Increasing" if recent_avg > older_avg * 1.1 else "📉 Decreasing" if recent_avg < older_avg * 0.9 else "➡️ Stable"
                    
                    console.print(f"\n[bold]Usage Trend:[/bold] {trend}")
                    if recent_avg > older_avg * 1.1:
                        console.print(f"[yellow]Recent usage is {((recent_avg/older_avg-1)*100):.1f}% higher than earlier period[/yellow]")
                    elif recent_avg < older_avg * 0.9:
                        console.print(f"[green]Recent usage is {((1-recent_avg/older_avg)*100):.1f}% lower than earlier period[/green]")

    except (APIError, ValueError) as e:
        click.echo(f"❌ {e}", err=True)

@siem_logs_group.command(name='overview')
@click.option('--time-range', help='Time range for usage data (defaults to Last 7 Days)')
@click.option('--output', type=click.Choice(['table', 'json']), help='Output format')
@click.option('--no-cache', is_flag=True, help='Disable caching')
@click.pass_context
def logs_overview(ctx, time_range, output, no_cache):
    """Unified view of logs, logsets, and usage data"""
    client, config_manager = get_client_and_config(ctx)
    use_json = should_use_json_output(output, config_manager.get('default_output'))
    
    if not time_range:
        time_range = "Last 7 Days"
    
    try:
        # Get logs list with logset information
        base_url = client.get_base_url('idr')
        logs_url = f"{base_url}/management/logs"
        logs_response = client.make_request("GET", logs_url)
        if logs_response.status_code != 200:
            raise APIError(f"Failed to list logs: {logs_response.status_code}")
        logs_data = logs_response.json()['logs']
        
        # Get usage data
        cache_key = f"overview_usage_{time_range}"
        cached_usage = None
        if client.cache_manager and not no_cache:
            cached_usage = client.cache_manager.get('log_usage', cache_key)
        
        if not cached_usage:
            usage_data = client.get_log_usage_by_log(time_range=time_range)
            if client.cache_manager and not no_cache:
                client.cache_manager.set('log_usage', cache_key, usage_data)
        else:
            usage_data = cached_usage
            if not use_json:
                console.print("📋 Using cached usage data", style="dim")
        
        # Build usage lookup by log ID
        usage_lookup = {}
        per_day_usage = usage_data.get('per_day_usage', {})
        for day_data in per_day_usage.get('usage', []):
            if isinstance(day_data, dict) and 'log_usage' in day_data:
                for log_entry in day_data['log_usage']:
                    log_id = log_entry.get('id')
                    usage = log_entry.get('usage', 0)
                    if log_id:
                        usage_lookup[log_id] = usage_lookup.get(log_id, 0) + usage
        
        # Calculate total usage
        total_usage = sum(usage_lookup.values())
        
        if use_json:
            # Combine data for JSON output
            result = {
                'period': per_day_usage.get('period', {}),
                'total_usage': total_usage,
                'logsets': {}
            }
            
            # Group by logset
            for log in logs_data:
                log_id = log.get('id')
                log_name = log.get('name', log_id)
                usage = usage_lookup.get(log_id, 0)
                
                logsets_info = log.get('logsets_info', [])
                if logsets_info:
                    for logset in logsets_info:
                        logset_name = logset.get('name', 'Unknown')
                        if logset_name not in result['logsets']:
                            result['logsets'][logset_name] = []
                        result['logsets'][logset_name].append({
                            'name': log_name,
                            'id': log_id,
                            'usage_bytes': usage
                        })
                else:
                    if 'No Logset' not in result['logsets']:
                        result['logsets']['No Logset'] = []
                    result['logsets']['No Logset'].append({
                        'name': log_name,
                        'id': log_id,
                        'usage_bytes': usage
                    })
            
            click.echo(json.dumps(result, indent=2))
        else:
            # Display unified table
            period_info = per_day_usage.get('period', {})
            
            # Calculate daily average
            period_from = period_info.get('from', '')
            period_to = period_info.get('to', '')
            daily_avg = 0
            
            if period_from and period_to:
                try:
                    from datetime import datetime
                    start_date = datetime.strptime(period_from, '%Y-%m-%d')
                    end_date = datetime.strptime(period_to, '%Y-%m-%d')
                    days = (end_date - start_date).days + 1  # +1 to include both start and end days
                    if days > 0:
                        daily_avg = total_usage / days
                except:
                    daily_avg = 0
            
            # Show summary first with consistent width
            summary_table = Table(title="SIEM Logs Overview", width=113)  # Match main table width
            summary_table.add_column("Metric", style="cyan", width=25)
            summary_table.add_column("Value", style="white", width=85)
            
            summary_table.add_row("Period", f"{period_from} to {period_to}")
            summary_table.add_row("Total Usage", format_bytes(total_usage))
            summary_table.add_row("Daily Average", format_bytes(daily_avg))
            summary_table.add_row("Total Logs", str(len(logs_data)))
            
            console.print(summary_table)
            console.print()
            
            # Create unified logset/usage table
            table = Table(title="Logsets, Logs & Usage")
            table.add_column("Logset / Log Name", style="cyan", width=45)
            table.add_column("Log ID", style="dim", width=36)
            table.add_column("Usage", style="yellow", justify="right", width=12)
            table.add_column("% of Total", style="green", justify="right", width=8)
            
            # Group logs by logset and sort by usage
            logset_groups = {}
            logs_without_logsets = []
            
            for log in logs_data:
                log_id = log.get('id')
                log_name = log.get('name', log_id)
                usage = usage_lookup.get(log_id, 0)
                
                log_info = {
                    'name': log_name,
                    'id': log_id,
                    'usage': usage
                }
                
                logsets_info = log.get('logsets_info', [])
                if logsets_info:
                    for logset in logsets_info:
                        logset_name = logset.get('name', 'Unknown')
                        if logset_name not in logset_groups:
                            logset_groups[logset_name] = []
                        logset_groups[logset_name].append(log_info)
                else:
                    logs_without_logsets.append(log_info)
            
            # Sort logsets by total usage (sum of their logs)
            def get_logset_total_usage(logs_list):
                return sum(log['usage'] for log in logs_list)
            
            sorted_logsets = sorted(logset_groups.items(), 
                                  key=lambda x: get_logset_total_usage(x[1]), 
                                  reverse=True)
            
            # Display each logset with its logs
            for logset_name, logs in sorted_logsets:
                # Logset header
                logset_total = get_logset_total_usage(logs)
                logset_pct = (logset_total / total_usage * 100) if total_usage > 0 else 0
                
                table.add_row(
                    f"[bold magenta]📁 {logset_name}[/bold magenta]",
                    "",
                    f"[bold]{format_bytes(logset_total)}[/bold]",
                    f"[bold]{logset_pct:.1f}%[/bold]"
                )
                
                # Sort logs within logset by usage
                sorted_logs = sorted(logs, key=lambda x: x['usage'], reverse=True)
                
                for log in sorted_logs:
                    usage_pct = (log['usage'] / total_usage * 100) if total_usage > 0 else 0
                    usage_display = format_bytes(log['usage']) if log['usage'] > 0 else "-"
                    
                    # Better percentage display for small values
                    if log['usage'] > 0:
                        if usage_pct >= 0.1:
                            pct_display = f"{usage_pct:.1f}%"
                        elif usage_pct >= 0.01:
                            pct_display = f"{usage_pct:.2f}%"
                        elif usage_pct >= 0.001:
                            pct_display = f"{usage_pct:.3f}%"
                        else:
                            pct_display = "<0.001%"
                    else:
                        pct_display = "-"
                    
                    table.add_row(
                        f"  ├─ {log['name']}",
                        log['id'],
                        usage_display,
                        pct_display
                    )
                
                # Add spacing
                table.add_row("", "", "", "")
            
            # Handle logs without logsets
            if logs_without_logsets:
                logset_total = sum(log['usage'] for log in logs_without_logsets)
                logset_pct = (logset_total / total_usage * 100) if total_usage > 0 else 0
                
                table.add_row(
                    "[bold dim]📁 No Logset[/bold dim]",
                    "",
                    f"[bold]{format_bytes(logset_total)}[/bold]",
                    f"[bold]{logset_pct:.1f}%[/bold]"
                )
                
                sorted_logs = sorted(logs_without_logsets, key=lambda x: x['usage'], reverse=True)
                for log in sorted_logs:
                    usage_pct = (log['usage'] / total_usage * 100) if total_usage > 0 else 0
                    usage_display = format_bytes(log['usage']) if log['usage'] > 0 else "-"
                    
                    # Better percentage display for small values
                    if log['usage'] > 0:
                        if usage_pct >= 0.1:
                            pct_display = f"{usage_pct:.1f}%"
                        elif usage_pct >= 0.01:
                            pct_display = f"{usage_pct:.2f}%"
                        elif usage_pct >= 0.001:
                            pct_display = f"{usage_pct:.3f}%"
                        else:
                            pct_display = "<0.001%"
                    else:
                        pct_display = "-"
                    
                    table.add_row(
                        f"  ├─ {log['name']}",
                        log['id'],
                        usage_display,
                        pct_display
                    )
            
            console.print(table)
            
            # Show helpful commands
            console.print("\n[dim]💡 Next steps:[/dim]")
            console.print("[dim]• Explore log fields: [/dim][cyan]r7 siem logs topkeys <LOG_ID>[/cyan]")
            console.print("[dim]• Sample log data: [/dim][cyan]r7 siem logs query <LOG_ID> 'limit(5)'[/cyan]")
            console.print("[dim]• Search all logs: [/dim][cyan]r7 siem logs query-all 'your_query'[/cyan]")
            
    except (APIError, ValueError) as e:
        click.echo(f"❌ {e}", err=True)

@siem_logs_group.command(name='topkeys')
@click.argument('log_name_or_id')
@click.option('--output', type=click.Choice(['table', 'json']), help='Output format')
@click.option('--no-cache', is_flag=True, help='Disable caching for this query')
@click.option('--limit', type=int, default=80, help='Limit the number of results displayed (default: 80, use -1 for all)')
@click.pass_context
def topkeys(ctx, log_name_or_id, output, no_cache, limit):
    """Retrieve the most common keys for a log"""
    client, config_manager = get_client_and_config(ctx)
    use_json = should_use_json_output(output, config_manager.get('default_output'))
    
    try:
        base_url = client.get_base_url('idr')
        
        # Resolve log name to ID if needed
        if not client.is_uuid(log_name_or_id):
            log_id = client.get_log_id_by_name(log_name_or_id)
        else:
            log_id = log_name_or_id
        
        # Check cache first
        cache_key = f"topkeys_{log_id}"
        cached_result = None
        if client.cache_manager and not no_cache:
            cached_result = client.cache_manager.get('topkeys', cache_key)
            if cached_result:
                if not use_json:
                    console.print("📋 Using cached result", style="dim")
        
        if not cached_result:
            url = f"{base_url}/management/logs/{log_id}/topkeys"
            response = client.make_request("GET", url)
            
            if response.status_code != 200:
                raise APIError(f"Failed to get top keys: {response.status_code} - {response.text}")
            
            data = response.json()
            
            # Cache the result
            if client.cache_manager and not no_cache:
                client.cache_manager.set('topkeys', cache_key, data)
        else:
            data = cached_result
        
        if use_json:
            # Apply limit to JSON output if specified (-1 means no limit)
            if limit != -1:
                limited_data = data.copy()
                keys_data = limited_data.get('topkeys', [])
                sorted_keys = sorted(keys_data, key=lambda x: x.get('weight', 0), reverse=True)
                limited_data['topkeys'] = sorted_keys[:limit]
                click.echo(json.dumps(limited_data, indent=2))
            else:
                click.echo(json.dumps(data, indent=2))
        else:
            # Display in table format
            keys_data = data.get('topkeys', [])
            
            if not keys_data:
                console.print("No key data found for this log", style="yellow")
                return
            
            # Get log name for display
            log_display_name = log_name_or_id
            try:
                if client.is_uuid(log_name_or_id):
                    # Try to get the actual log name
                    logs_url = f"{base_url}/management/logs"
                    logs_response = client.make_request("GET", logs_url)
                    if logs_response.status_code == 200:
                        logs_list = logs_response.json().get('logs', [])
                        for log in logs_list:
                            if log.get('id') == log_id:
                                log_display_name = log.get('name', log_name_or_id)
                                break
            except Exception:
                pass  # Fall back to original name/id
            
            table = Table(title=f"Most Common Keys: {log_display_name}")
            table.add_column("Rank", style="cyan", width=6)
            table.add_column("Key Name", style="white", width=60)
            table.add_column("Weight", style="yellow", justify="right", width=12)
            table.add_column("Relative Frequency", style="green", width=30, no_wrap=True)
            
            # Sort by weight (descending) and add ranking
            sorted_keys = sorted(keys_data, key=lambda x: x.get('weight', 0), reverse=True)
            total_keys = len(sorted_keys)
            
            # Apply limit if specified (-1 means no limit)
            was_truncated = False
            if limit != -1 and len(sorted_keys) > limit:
                sorted_keys = sorted_keys[:limit]
                was_truncated = True
            
            max_weight = max((key.get('weight', 0) for key in sorted_keys), default=1)
            
            for rank, key_info in enumerate(sorted_keys, 1):
                key_name = key_info.get('key', 'Unknown')
                weight = key_info.get('weight', 0)
                
                # Calculate relative frequency as percentage of max weight
                relative_freq = (weight / max_weight * 100) if max_weight > 0 else 0
                
                # Create visual indicator
                bar_length = int(relative_freq / 10)  # Scale to 0-10 chars
                visual_bar = "█" * bar_length + "░" * (10 - bar_length)
                
                table.add_row(
                    str(rank),
                    key_name,
                    f"{weight:.2f}",
                    f"{visual_bar} {relative_freq:.1f}%"
                )
            
            console.print(table)
            
            # Show truncation message if needed
            if was_truncated:
                console.print(f"\n[yellow]⚠️  Showing top {limit} keys out of {total_keys} total.[/yellow]")
                console.print(f"[dim]Use --limit -1 to see all keys, or --limit N to see a specific number.[/dim]")
            
            # Show summary stats
            total_keys_shown = len(sorted_keys)
            avg_weight = sum(key.get('weight', 0) for key in keys_data) / len(keys_data) if keys_data else 0
            
            summary_table = Table(title="Summary Statistics")
            summary_table.add_column("Metric", style="cyan")
            summary_table.add_column("Value", style="white")
            
            summary_table.add_row("Total Unique Keys", str(len(keys_data)))
            summary_table.add_row("Average Weight", f"{avg_weight:.2f}")
            summary_table.add_row("Max Weight", f"{max_weight:.2f}")
            
            if sorted_keys:
                summary_table.add_row("Most Common Key", sorted_keys[0].get('key', 'Unknown'))
            
            console.print(summary_table)
            
            if total_keys >= 1000:
                console.print("[dim]Note: Only the 1000 most common keys are returned by the API[/dim]")
    
    except (APIError, QueryError) as e:
        click.echo(f"❌ {e}", err=True)