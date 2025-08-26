import sys
import json
import os
import time
import click
import requests
from datetime import datetime
from rich.console import Console
from rich.table import Table
from rich.progress import Progress, SpinnerColumn, TextColumn, TimeElapsedColumn, BarColumn, DownloadColumn, TransferSpeedColumn

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


# --- Bulk Export Commands ---
@click.group(name='bulk-export')
def bulk_export_group():
    """Bulk export policies and vulnerabilities to Parquet files"""
    pass


@bulk_export_group.command(name='policy')
@click.option('--output-dir', '-o', type=click.Path(), default='./exports', help='Directory to save exported files')
@click.option('--no-download', is_flag=True, help='Only initiate export without waiting or downloading')
@click.option('--output', type=click.Choice(['table', 'json']), help='Output format')
@click.pass_context
def export_policy(ctx, output_dir, no_download, output):
    """Export agent-based policies and assets to Parquet files
    
    By default, this command will:
    1. Initiate the export
    2. Wait for it to complete (with progress indicator)
    3. Automatically download the files
    
    Use --no-download to only initiate the export without waiting.
    """
    try:
        client = _get_vm_cloud_client(ctx)
        config = ConfigManager()
        use_format = determine_output_format(output, config)
        
        # Step 1: Initiate export
        console.print("[yellow]📤 Initiating policy export...[/yellow]")
        result = client.create_policy_export()
        
        export_id = result.get('id')
        if not export_id:
            raise APIError("Failed to get export ID from response")
        
        console.print(f"[green]✅ Export initiated[/green] - ID: [cyan]{export_id}[/cyan]")
        
        if no_download:
            # User just wants to initiate
            if use_format == 'json':
                click.echo(json.dumps({'export_id': export_id, 'status': 'initiated'}, indent=2))
            else:
                console.print(f"\nExport ID: [cyan]{export_id}[/cyan]")
                console.print(f"Check status: [dim]r7 vm bulk-export status {export_id}[/dim]")
                console.print(f"Download later: [dim]r7 vm bulk-export download {export_id}[/dim]")
            return
        
        # Step 2: Wait for completion with progress
        console.print("[yellow]⏳ Waiting for export to complete...[/yellow]")
        
        with Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            TimeElapsedColumn(),
            console=console
        ) as progress:
            task = progress.add_task("Processing export...", total=None)
            
            poll_count = 0
            max_polls = 360  # 30 minutes max (5 second intervals)
            
            while poll_count < max_polls:
                export_data = client.get_export_status(export_id)
                status = export_data.get('status', 'UNKNOWN')
                
                if status in ['COMPLETE', 'SUCCEEDED']:
                    progress.update(task, description="[green]Export complete![/green]")
                    break
                elif status == 'FAILED':
                    progress.update(task, description="[red]Export failed![/red]")
                    raise APIError(f"Export failed with status: {status}")
                else:
                    progress.update(task, description=f"Status: {status}...")
                    time.sleep(5)
                    poll_count += 1
            
            if poll_count >= max_polls:
                raise APIError("Export timed out after 30 minutes")
        
        # Step 3: Automatically download the files
        console.print("[yellow]📥 Downloading exported files...[/yellow]")
        
        result = export_data.get('result', [])
        urls = []
        if isinstance(result, list):
            for item in result:
                if isinstance(item, dict) and 'urls' in item:
                    urls.extend(item['urls'])
        elif isinstance(result, dict) and 'urls' in result:
            urls = result['urls']
        
        if not urls:
            console.print("[yellow]⚠️ Export complete but no files available[/yellow]")
            return
        
        # Create output directory
        output_path = os.path.expanduser(output_dir)
        os.makedirs(output_path, exist_ok=True)
        
        # Create subdirectory for this export
        dataset = export_data.get('dataset', 'policy')
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        export_dir = os.path.join(output_path, f"{dataset.lower()}_{timestamp}_{export_id[:8]}")
        os.makedirs(export_dir, exist_ok=True)
        
        console.print(f"Downloading {len(urls)} files to: [cyan]{export_dir}[/cyan]\n")
        
        # Download each file
        headers = {'x-api-key': client.api_key}
        downloaded_files = []
        
        for i, url in enumerate(urls, 1):
            filename = f"export_{i:03d}.parquet"
            if '/' in url:
                potential_name = url.split('/')[-1].split('?')[0]
                if potential_name:
                    filename = potential_name
            
            filepath = os.path.join(export_dir, filename)
            
            try:
                response = requests.get(url, headers=headers, stream=True)
                response.raise_for_status()
                
                total_size = int(response.headers.get('content-length', 0))
                
                with open(filepath, 'wb') as f:
                    with Progress(
                        TextColumn("[progress.description]{task.description}"),
                        BarColumn(),
                        DownloadColumn(),
                        TransferSpeedColumn(),
                        console=console
                    ) as progress:
                        task = progress.add_task(f"[cyan]{filename}[/cyan]", total=total_size)
                        
                        for chunk in response.iter_content(chunk_size=8192):
                            f.write(chunk)
                            progress.update(task, advance=len(chunk))
                
                downloaded_files.append(filename)
                
            except requests.exceptions.RequestException as e:
                console.print(f"[red]❌ Failed to download {filename}: {e}[/red]")
                continue
        
        # Final summary
        console.print(f"\n[green]✅ Export complete![/green]")
        console.print(f"Downloaded {len(downloaded_files)}/{len(urls)} files to:")
        console.print(f"[cyan]{export_dir}[/cyan]")
        
        if use_format == 'json':
            click.echo(json.dumps({
                'export_id': export_id,
                'status': 'complete',
                'files_downloaded': len(downloaded_files),
                'output_directory': export_dir
            }, indent=2))
            
    except (APIError, AuthenticationError, ConfigurationError) as e:
        click.echo(f"❌ {e}", err=True)


@bulk_export_group.command(name='vulns')
@click.option('--output-dir', '-o', type=click.Path(), default='./exports', help='Directory to save exported files')
@click.option('--no-download', is_flag=True, help='Only initiate export without waiting or downloading')
@click.option('--output', type=click.Choice(['table', 'json']), help='Output format')
@click.pass_context
def export_vulnerabilities(ctx, output_dir, no_download, output):
    """Export all vulnerabilities and assets to Parquet files
    
    By default, this command will:
    1. Initiate the export
    2. Wait for it to complete (with progress indicator)
    3. Automatically download the files
    
    Use --no-download to only initiate the export without waiting.
    """
    try:
        client = _get_vm_cloud_client(ctx)
        config = ConfigManager()
        use_format = determine_output_format(output, config)
        
        # Step 1: Initiate export
        console.print("[yellow]📤 Initiating vulnerability export...[/yellow]")
        result = client.create_vulnerability_export()
        
        export_id = result.get('id')
        if not export_id:
            raise APIError("Failed to get export ID from response")
        
        console.print(f"[green]✅ Export initiated[/green] - ID: [cyan]{export_id}[/cyan]")
        
        if no_download:
            # User just wants to initiate
            if use_format == 'json':
                click.echo(json.dumps({'export_id': export_id, 'status': 'initiated'}, indent=2))
            else:
                console.print(f"\nExport ID: [cyan]{export_id}[/cyan]")
                console.print(f"Check status: [dim]r7 vm bulk-export status {export_id}[/dim]")
                console.print(f"Download later: [dim]r7 vm bulk-export download {export_id}[/dim]")
            return
        
        # Step 2: Wait for completion with progress
        console.print("[yellow]⏳ Waiting for export to complete...[/yellow]")
        
        with Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            TimeElapsedColumn(),
            console=console
        ) as progress:
            task = progress.add_task("Processing export...", total=None)
            
            poll_count = 0
            max_polls = 360  # 30 minutes max (5 second intervals)
            
            while poll_count < max_polls:
                export_data = client.get_export_status(export_id)
                status = export_data.get('status', 'UNKNOWN')
                
                if status in ['COMPLETE', 'SUCCEEDED']:
                    progress.update(task, description="[green]Export complete![/green]")
                    break
                elif status == 'FAILED':
                    progress.update(task, description="[red]Export failed![/red]")
                    raise APIError(f"Export failed with status: {status}")
                else:
                    progress.update(task, description=f"Status: {status}...")
                    time.sleep(5)
                    poll_count += 1
            
            if poll_count >= max_polls:
                raise APIError("Export timed out after 30 minutes")
        
        # Step 3: Automatically download the files
        console.print("[yellow]📥 Downloading exported files...[/yellow]")
        
        result = export_data.get('result', [])
        urls = []
        if isinstance(result, list):
            for item in result:
                if isinstance(item, dict) and 'urls' in item:
                    urls.extend(item['urls'])
        elif isinstance(result, dict) and 'urls' in result:
            urls = result['urls']
        
        if not urls:
            console.print("[yellow]⚠️ Export complete but no files available[/yellow]")
            return
        
        # Create output directory
        output_path = os.path.expanduser(output_dir)
        os.makedirs(output_path, exist_ok=True)
        
        # Create subdirectory for this export
        dataset = export_data.get('dataset', 'vulnerability')
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        export_dir = os.path.join(output_path, f"{dataset.lower()}_{timestamp}_{export_id[:8]}")
        os.makedirs(export_dir, exist_ok=True)
        
        console.print(f"Downloading {len(urls)} files to: [cyan]{export_dir}[/cyan]\n")
        
        # Download each file
        headers = {'x-api-key': client.api_key}
        downloaded_files = []
        
        for i, url in enumerate(urls, 1):
            filename = f"export_{i:03d}.parquet"
            if '/' in url:
                potential_name = url.split('/')[-1].split('?')[0]
                if potential_name:
                    filename = potential_name
            
            filepath = os.path.join(export_dir, filename)
            
            try:
                response = requests.get(url, headers=headers, stream=True)
                response.raise_for_status()
                
                total_size = int(response.headers.get('content-length', 0))
                
                with open(filepath, 'wb') as f:
                    with Progress(
                        TextColumn("[progress.description]{task.description}"),
                        BarColumn(),
                        DownloadColumn(),
                        TransferSpeedColumn(),
                        console=console
                    ) as progress:
                        task = progress.add_task(f"[cyan]{filename}[/cyan]", total=total_size)
                        
                        for chunk in response.iter_content(chunk_size=8192):
                            f.write(chunk)
                            progress.update(task, advance=len(chunk))
                
                downloaded_files.append(filename)
                
            except requests.exceptions.RequestException as e:
                console.print(f"[red]❌ Failed to download {filename}: {e}[/red]")
                continue
        
        # Final summary
        console.print(f"\n[green]✅ Export complete![/green]")
        console.print(f"Downloaded {len(downloaded_files)}/{len(urls)} files to:")
        console.print(f"[cyan]{export_dir}[/cyan]")
        
        if use_format == 'json':
            click.echo(json.dumps({
                'export_id': export_id,
                'status': 'complete',
                'files_downloaded': len(downloaded_files),
                'output_directory': export_dir
            }, indent=2))
            
    except (APIError, AuthenticationError, ConfigurationError) as e:
        click.echo(f"❌ {e}", err=True)


@bulk_export_group.command(name='status')
@click.argument('export_id', required=True)
@click.option('--wait/--no-wait', default=False, help='Wait for export to complete')
@click.option('--output', type=click.Choice(['table', 'json']), help='Output format')
@click.pass_context
def export_status(ctx, export_id, wait, output):
    """Check the status of a bulk export
    
    Query the status of an export and get download URLs when ready.
    Use --wait to poll until the export is complete.
    """
    try:
        client = _get_vm_cloud_client(ctx)
        config = ConfigManager()
        use_format = determine_output_format(output, config)
        
        if wait:
            # Poll with progress indicator
            with Progress(
                SpinnerColumn(),
                TextColumn("[progress.description]{task.description}"),
                TimeElapsedColumn(),
                console=console
            ) as progress:
                task = progress.add_task("Waiting for export to complete...", total=None)
                
                while True:
                    export_data = client.get_export_status(export_id)
                    status = export_data.get('status', 'UNKNOWN')
                    
                    if status == 'COMPLETE':
                        progress.update(task, description="Export complete!")
                        break
                    elif status == 'FAILED':
                        progress.update(task, description="Export failed!")
                        raise APIError(f"Export failed with status: {status}")
                    else:
                        progress.update(task, description=f"Export status: {status}...")
                        time.sleep(5)  # Poll every 5 seconds
        else:
            export_data = client.get_export_status(export_id)
        
        if use_format == 'json':
            click.echo(json.dumps(export_data, indent=2))
        else:
            # Display export details in a table
            status = export_data.get('status', 'UNKNOWN')
            dataset = export_data.get('dataset', 'UNKNOWN')
            timestamp = export_data.get('timestamp', '')
            
            table = Table(title=f"Export Status - {export_id[:12]}...")
            table.add_column('Field', style='cyan')
            table.add_column('Value', style='white')
            
            table.add_row('Export ID', export_id)
            table.add_row('Status', status)
            table.add_row('Dataset', dataset)
            table.add_row('Timestamp', timestamp)
            
            console.print(table)
            
            # Show download URLs if available
            result = export_data.get('result', [])
            urls = []
            if isinstance(result, list):
                for item in result:
                    if isinstance(item, dict) and 'urls' in item:
                        urls.extend(item['urls'])
            elif isinstance(result, dict) and 'urls' in result:
                urls = result['urls']
            
            if urls:
                console.print(f"\n[green]✅ Export ready for download![/green]")
                
                # Show breakdown by data type with file info
                if isinstance(result, list):
                    for item in result:
                        if isinstance(item, dict) and 'urls' in item and 'prefix' in item:
                            prefix = item['prefix']
                            item_urls = item['urls']
                            
                            # Make prefix more human-readable
                            if prefix == 'asset':
                                data_type = "Asset data"
                            elif prefix == 'asset_vulnerability':
                                data_type = "Vulnerability findings"
                            elif prefix == 'asset_policy':
                                data_type = "Policy compliance"
                            else:
                                data_type = f"{prefix.replace('_', ' ').title()} data"
                            
                            console.print(f"  📄 {data_type}: {len(item_urls)} file{'s' if len(item_urls) != 1 else ''}")
                else:
                    console.print(f"Number of files: {len(urls)}")
                
                console.print(f"\n[dim]💡 Download with: r7 vm bulk-export download {export_id}[/dim]")
            elif status in ['COMPLETE', 'SUCCEEDED']:
                console.print("\n[yellow]⚠️ Export complete but no files available[/yellow]")
            elif status == 'FAILED':
                console.print("\n[red]❌ Export failed[/red]")
            else:
                console.print(f"\n[yellow]Export is still processing... Check again later[/yellow]")
                
    except (APIError, AuthenticationError, ConfigurationError) as e:
        click.echo(f"❌ {e}", err=True)


@bulk_export_group.command(name='download')
@click.argument('export_id', required=True)
@click.option('--output-dir', '-o', type=click.Path(), default='./exports', help='Directory to save exported files')
@click.option('--show-urls/--no-show-urls', default=False, help='Show download URLs without downloading')
@click.pass_context
def download_export(ctx, export_id, output_dir, show_urls):
    """Download exported Parquet files
    
    Downloads all Parquet files from a completed export.
    Files are saved to the specified output directory.
    
    Note: Download URLs are valid for 15 minutes after generation.
    """
    try:
        client = _get_vm_cloud_client(ctx)
        
        # Get export status and URLs
        console.print("[yellow]Fetching export details...[/yellow]")
        export_data = client.get_export_status(export_id)
        
        status = export_data.get('status', 'UNKNOWN')
        if status not in ['COMPLETE', 'SUCCEEDED']:
            raise APIError(f"Export is not ready for download. Current status: {status}")
        
        result = export_data.get('result', [])
        urls = []
        if isinstance(result, list):
            for item in result:
                if isinstance(item, dict) and 'urls' in item:
                    urls.extend(item['urls'])
        elif isinstance(result, dict) and 'urls' in result:
            urls = result['urls']
        # prefix is not needed for download functionality
        
        if not urls:
            console.print("[yellow]⚠️ No files available for download[/yellow]")
            return
        
        if show_urls:
            # Just display URLs with better organization
            console.print(f"\n[cyan]Download URLs (valid for 15 minutes):[/cyan]")
            
            if isinstance(result, list):
                url_index = 1
                for item in result:
                    if isinstance(item, dict) and 'urls' in item and 'prefix' in item:
                        prefix = item['prefix']
                        item_urls = item['urls']
                        
                        # Make prefix more human-readable
                        if prefix == 'asset':
                            data_type = "Asset data"
                        elif prefix == 'asset_vulnerability':
                            data_type = "Vulnerability findings"
                        elif prefix == 'asset_policy':
                            data_type = "Policy compliance"
                        else:
                            data_type = f"{prefix.replace('_', ' ').title()} data"
                        
                        console.print(f"\n[bold]{data_type}:[/bold]")
                        for url in item_urls:
                            console.print(f"{url_index}. {url}")
                            url_index += 1
            else:
                for i, url in enumerate(urls, 1):
                    console.print(f"{i}. {url}")
            
            console.print(f"\n[dim]Total files: {len(urls)}[/dim]")
            return
        
        # Create output directory
        output_path = os.path.expanduser(output_dir)
        os.makedirs(output_path, exist_ok=True)
        
        # Create subdirectory for this export
        dataset = export_data.get('dataset', 'unknown')
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        export_dir = os.path.join(output_path, f"{dataset}_{timestamp}_{export_id[:8]}")
        os.makedirs(export_dir, exist_ok=True)
        
        console.print(f"\n[green]Downloading {len(urls)} files to: {export_dir}[/green]\n")
        
        # Download each file with progress bar
        headers = {'x-api-key': client.api_key}
        
        for i, url in enumerate(urls, 1):
            # Extract filename from URL or create one
            filename = f"export_{i:03d}.parquet"
            if '/' in url:
                potential_name = url.split('/')[-1].split('?')[0]
                if potential_name:
                    filename = potential_name
            
            filepath = os.path.join(export_dir, filename)
            
            # Download with progress bar
            try:
                response = requests.get(url, headers=headers, stream=True)
                response.raise_for_status()
                
                total_size = int(response.headers.get('content-length', 0))
                
                with open(filepath, 'wb') as f:
                    with Progress(
                        TextColumn("[progress.description]{task.description}"),
                        BarColumn(),
                        DownloadColumn(),
                        TransferSpeedColumn(),
                        console=console
                    ) as progress:
                        task = progress.add_task(f"[cyan]{filename}", total=total_size)
                        
                        for chunk in response.iter_content(chunk_size=8192):
                            f.write(chunk)
                            progress.update(task, advance=len(chunk))
                
                console.print(f"✅ Downloaded: {filename}")
                
            except requests.exceptions.RequestException as e:
                console.print(f"[red]❌ Failed to download {filename}: {e}[/red]")
                continue
        
        console.print(f"\n[green]✅ Export download complete![/green]")
        console.print(f"Files saved to: [cyan]{export_dir}[/cyan]")
        console.print(f"\n[dim]Note: These Parquet files can be imported into data analysis tools like pandas, Apache Spark, or BI platforms.[/dim]")
        
    except (APIError, AuthenticationError, ConfigurationError) as e:
        click.echo(f"❌ {e}", err=True)