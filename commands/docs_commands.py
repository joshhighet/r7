import json
import sys
import click
from rich.console import Console
from rich.table import Table
from rich.progress import Progress, SpinnerColumn, TextColumn, TimeElapsedColumn
from api.client import DocsClient
from utils.config import ConfigManager
from utils.cache import CacheManager
from utils.exceptions import *
console = Console()
@click.command(name='docs')
@click.argument('search_query')
@click.option('--limit', default=15, type=int, help='Maximum number of results to return (default: 15)')
@click.option('--output', type=click.Choice(['simple', 'table', 'json']), help='Output format')
@click.option('--no-cache', is_flag=True, help='Disable caching for this search')
@click.pass_context
def docs(ctx, search_query, limit, output, no_cache):
    """search docs.rapid7.com content"""
    try:
        config_manager = ConfigManager()
        config_manager.validate()
      
        cache_manager = None
        if config_manager.get('cache_enabled') and not no_cache:
            cache_manager = CacheManager(ttl=config_manager.get('cache_ttl'))
      
        docs_client = DocsClient(cache_manager)
      
        # Determine output format
        if output:
            use_format = output
        elif not sys.stdout.isatty(): # Piped output
            use_format = 'simple'
        else:
            use_format = config_manager.get('default_output', 'simple')
      
        # Perform search
        if use_format != 'json':
            with Progress(
                SpinnerColumn(),
                TextColumn("Searching documentation..."),
                TimeElapsedColumn(),
            ) as progress:
                task = progress.add_task("Searching...", total=None)
                results = docs_client.search_docs(search_query, limit)
        else:
            results = docs_client.search_docs(search_query, limit)
      
        # Display results
        if use_format == 'json':
            click.echo(json.dumps(results, indent=2))
        elif use_format == 'table':
            if results:
                table = Table(title=f"Documentation Search: '{search_query}'")
                table.add_column("Title", style="cyan", width=40)
                table.add_column("Product", style="yellow", width=15)
                table.add_column("URL", style="blue", no_wrap=False)
              
                for result in results:
                    # Truncate title if too long
                    title = result['title']
                    if len(title) > 37:
                        title = title[:34] + "..."
                  
                    table.add_row(
                        title,
                        result['product'],
                        result['url']
                    )
              
                console.print(table)
              
                if results:
                    console.print(f"\n[dim]Found {len(results)} result(s). Use --output json to see descriptions.[/dim]")
            else:
                console.print("No documentation found matching your search", style="yellow")
        else: # simple format
            if results:
                for result in results:
                    click.echo(f"{result['title']} - {result['url']}")
            else:
                click.echo("No documentation found matching your search")
              
    except (APIError, ConfigurationError) as e:
        click.echo(f"❌ {e}", err=True)

