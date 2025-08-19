import json
import sys
import click
from rich.console import Console
from rich.table import Table

from utils.cli import ClientManager, OutputFormatter, common_output_options, error_handler
from utils.config import ConfigManager

console = Console()


@click.group(name='ic')
def ic_group():
    """manage automation jobs, execute workflows"""
    pass


# -----------------
# Workflows
# -----------------
@ic_group.group(name='workflows')
def workflows_group():
    """Manage InsightConnect workflows"""
    pass


@workflows_group.command(name='list')
@click.option('--limit', default=30, type=int, help='Number of items (max 30)')
@click.option('--offset', default=0, type=int, help='Offset for pagination')
@common_output_options
@click.pass_context
@error_handler
def list_workflows(ctx, limit, offset, output, no_cache):
    """List workflows"""
    client, config_manager = ClientManager().get_client_and_config(ctx, cache_namespace='ic')

    cache_key = f"workflows_{limit}_{offset}"
    def fetch():
        return client.ic_list_workflows(limit=limit, offset=offset)

    data = None
    if client.cache_manager and not no_cache:
        data = client.cache_manager.get('ic', cache_key)
        if data:
            OutputFormatter.display_cached_message()
    if not data:
        data = fetch()
        if client.cache_manager and not no_cache:
            client.cache_manager.set('ic', cache_key, data)

    def table_fmt(d):
        payload = d.get('data', d)
        items = payload.get('workflows', payload) if isinstance(payload, dict) else payload
        if not items:
            console.print("No workflows found", style="yellow")
            return
        table = OutputFormatter.create_standard_table("InsightConnect Workflows", [
            {'name': 'ID', 'style': 'cyan'},
            {'name': 'Name', 'style': 'white'},
            {'name': 'State', 'style': 'green'},
            {'name': 'Tags', 'style': 'yellow'},
        ])
        for wf in items:
            # workflow shape may already be flattened or under keys
            wf_id = wf.get('workflowId') or wf.get('id')
            state = wf.get('state', '')
            # name could be on publishedVersion or unpublishedVersion
            name = (wf.get('publishedVersion') or {}).get('name') or (wf.get('unpublishedVersion') or {}).get('name') or wf.get('name', '')
            tags = (wf.get('publishedVersion') or {}).get('tags') or (wf.get('unpublishedVersion') or {}).get('tags') or []
            table.add_row(str(wf_id or ''), str(name or ''), str(state or ''), ", ".join(tags) if isinstance(tags, list) else str(tags))
        console.print(table)

    def simple_fmt(d):
        payload = d.get('data', d)
        items = payload.get('workflows', payload) if isinstance(payload, dict) else payload
        if not items:
            click.echo("No workflows found")
            return
        for wf in items:
            wf_id = wf.get('workflowId') or wf.get('id')
            name = (wf.get('publishedVersion') or {}).get('name') or (wf.get('unpublishedVersion') or {}).get('name') or wf.get('name', '')
            click.echo(f"{name} {wf_id}")

    OutputFormatter.output_data(data, output, config_manager, table_formatter=table_fmt, simple_formatter=simple_fmt)


@workflows_group.command(name='get')
@click.argument('workflow_id')
@common_output_options
@click.pass_context
@error_handler
def get_workflow(ctx, workflow_id, output, no_cache):
    """Get a workflow by ID"""
    client, config_manager = ClientManager().get_client_and_config(ctx, cache_namespace='ic')
    cache_key = f"workflow_{workflow_id}"
    def fetch():
        return client.ic_get_workflow(workflow_id)

    data = None
    if client.cache_manager and not no_cache:
        data = client.cache_manager.get('ic', cache_key)
        if data:
            OutputFormatter.display_cached_message()
    if not data:
        data = fetch()
        if client.cache_manager and not no_cache:
            client.cache_manager.set('ic', cache_key, data)

    def table_fmt(d):
        wf = d.get('data', d)
        table = OutputFormatter.create_standard_table(f"Workflow {workflow_id}", [
            {'name': 'Field', 'style': 'cyan'},
            {'name': 'Value', 'style': 'white'},
        ])
        name = (wf.get('publishedVersion') or {}).get('name') or (wf.get('unpublishedVersion') or {}).get('name') or wf.get('name', '')
        state = wf.get('state', '')
        rrn = wf.get('rrn', '')
        table.add_row('Name', str(name))
        table.add_row('State', str(state))
        table.add_row('RRN', str(rrn))
        console.print(table)

    OutputFormatter.output_data(data, output, config_manager, table_formatter=table_fmt)


@workflows_group.command(name='run')
@click.argument('workflow_id')
@click.option('--param', 'params_', multiple=True, help='Key=Value parameter to pass; repeatable')
@click.option('--wait', is_flag=True, help='Wait for the resulting job to complete')
@click.option('--timeout', default=600, type=int, help='Max seconds to wait when --wait is used')
@click.option('--interval', default=3, type=int, help='Polling interval seconds')
@common_output_options
@click.pass_context
@error_handler
def run_workflow(ctx, workflow_id, params_, wait, timeout, interval, output, no_cache):
    """Execute a workflow's active version asynchronously."""
    client, config_manager = ClientManager().get_client_and_config(ctx, cache_namespace='ic')

    # Convert --param key=value pairs into a dict
    body = {}
    for kv in params_:
        if '=' not in kv:
            raise click.BadParameter(f"Invalid --param '{kv}', expected key=value")
        k, v = kv.split('=', 1)
        body.setdefault('parameters', {}).setdefault('values', {})[k] = v

    resp = client.ic_execute_workflow(workflow_id, body or None)

    # Attempt to find jobId in common places
    job_id = None
    try:
        # sometimes returns { data: { job: { jobId } } } or { jobId } directly
        job_id = resp.get('data', {}).get('job', {}).get('jobId') or resp.get('jobId')
    except Exception:
        job_id = None

    if wait and job_id:
        resp = client.ic_wait_for_job(job_id, timeout=timeout, interval=interval)

    def table_fmt(d):
        table = OutputFormatter.create_standard_table("Workflow Execution", [
            {'name': 'Field', 'style': 'cyan'},
            {'name': 'Value', 'style': 'white'},
        ])
        
        # Always show workflow ID
        table.add_row('Workflow ID', str(workflow_id))
        
        # Show execution status
        if d is None:
            table.add_row('Status', 'Execution Started (202 Accepted)')
            table.add_row('Response', 'Workflow execution accepted by server')
        else:
            table.add_row('Status', 'Execution Response Received')
        
        # Show job ID if found
        if job_id:
            table.add_row('Job ID', str(job_id))
            if not wait:
                table.add_row('Next Step', f'Use: r7 ic jobs get {job_id}')
        else:
            table.add_row('Job ID', 'Not returned in response')
            
        # Show job details if available (when --wait is used)
        try:
            job = d.get('data', {}).get('job', {}) if d else {}
            if job:
                for field in ['status', 'name', 'duration', 'startedAt', 'endedAt']:
                    val = job.get(field, '')
                    if val:
                        table.add_row(field, str(val))
        except Exception:
            pass
            
        console.print(table)

    OutputFormatter.output_data(resp, output, config_manager, table_formatter=table_fmt)


@workflows_group.command(name='on')
@click.argument('workflow_id')
@common_output_options
@click.pass_context
@error_handler
def turn_on_workflow(ctx, workflow_id, output, no_cache):
    """Turn on a workflow."""
    client, config_manager = ClientManager().get_client_and_config(ctx, cache_namespace='ic')
    data = client.ic_activate_workflow(workflow_id)
    
    def table_fmt(d):
        table = OutputFormatter.create_standard_table("Workflow Activation", [
            {'name': 'Field', 'style': 'cyan'},
            {'name': 'Value', 'style': 'white'},
        ])
        table.add_row('Workflow ID', str(workflow_id))
        table.add_row('Status', 'Activated')
        if isinstance(d, dict) and d.get('message'):
            table.add_row('Message', str(d.get('message')))
        console.print(table)
    
    OutputFormatter.output_data(data, output, config_manager, table_formatter=table_fmt)


@workflows_group.command(name='off')
@click.argument('workflow_id')
@common_output_options
@click.pass_context
@error_handler
def turn_off_workflow(ctx, workflow_id, output, no_cache):
    """Turn off a workflow."""
    client, config_manager = ClientManager().get_client_and_config(ctx, cache_namespace='ic')
    data = client.ic_inactivate_workflow(workflow_id)
    
    def table_fmt(d):
        table = OutputFormatter.create_standard_table("Workflow Deactivation", [
            {'name': 'Field', 'style': 'cyan'},
            {'name': 'Value', 'style': 'white'},
        ])
        table.add_row('Workflow ID', str(workflow_id))
        table.add_row('Status', 'Deactivated')
        if isinstance(d, dict) and d.get('message'):
            table.add_row('Message', str(d.get('message')))
        console.print(table)
    
    OutputFormatter.output_data(data, output, config_manager, table_formatter=table_fmt)


@workflows_group.command(name='export')
@click.argument('workflow_id')
@click.option('--exclude-config-details', is_flag=True, help='Exclude connections/params/notifications from export')
@common_output_options
@click.pass_context
@error_handler
def export_workflow(ctx, workflow_id, exclude_config_details, output, no_cache):
    """Export a workflow definition."""
    client, config_manager = ClientManager().get_client_and_config(ctx, cache_namespace='ic')
    data = client.ic_export_workflow(workflow_id, exclude_config_details)
    
    # Export should always default to JSON output since it's structured data to be used/saved
    # Override the output format to JSON unless explicitly set to table
    if output is None and sys.stdout.isatty():
        output = 'json'
    
    OutputFormatter.output_data(data, output, config_manager)


# -----------------
# Jobs
# -----------------
@ic_group.group(name='jobs')
def jobs_group():
    """Inspect InsightConnect jobs"""
    pass


@jobs_group.command(name='list')
@click.option('--limit', default=30, type=int, help='Number of items (max 30)')
@click.option('--offset', default=0, type=int, help='Offset for pagination')
@click.option('--status', type=click.Choice(['queued', 'running', 'succeeded', 'failed', 'canceled', 'cancelled']), help='Filter by status')
@common_output_options
@click.pass_context
@error_handler
def list_jobs(ctx, limit, offset, status, output, no_cache):
    """List jobs"""
    client, config_manager = ClientManager().get_client_and_config(ctx, cache_namespace='ic')
    cache_key = f"jobs_{limit}_{offset}_{status or 'all'}"
    def fetch():
        return client.ic_list_jobs(limit=limit, offset=offset, status=status)

    data = None
    if client.cache_manager and not no_cache:
        data = client.cache_manager.get('ic', cache_key)
        if data:
            OutputFormatter.display_cached_message()
    if not data:
        data = fetch()
        if client.cache_manager and not no_cache:
            client.cache_manager.set('ic', cache_key, data)

    def table_fmt(d):
        payload = d.get('data', d)
        items = payload.get('jobs', payload) if isinstance(payload, dict) else payload
        if not items:
            console.print("No jobs found", style="yellow")
            return
        table = OutputFormatter.create_standard_table("InsightConnect Jobs", [
            {'name': 'Job ID', 'style': 'cyan'},
            {'name': 'Name', 'style': 'white'},
            {'name': 'Status', 'style': 'green'},
            {'name': 'Duration (s)', 'style': 'yellow'},
        ])
        for jwrap in items:
            job = jwrap.get('job', jwrap)
            table.add_row(
                str(job.get('jobId') or job.get('id', '')),
                str(job.get('name', '')),
                str(job.get('status', '')),
                str(job.get('duration', '')),
            )
        console.print(table)

    def simple_fmt(d):
        payload = d.get('data', d)
        items = payload.get('jobs', payload) if isinstance(payload, dict) else payload
        if not items:
            click.echo("No jobs found")
            return
        for jwrap in items:
            job = jwrap.get('job', jwrap)
            click.echo(f"{job.get('name','')} {job.get('jobId','')} {job.get('status','')}")

    OutputFormatter.output_data(data, output, config_manager, table_formatter=table_fmt, simple_formatter=simple_fmt)


@jobs_group.command(name='get')
@click.argument('job_id')
@click.option('--wait', is_flag=True, help='Wait for job to reach a terminal state')
@click.option('--timeout', default=600, type=int, help='Max seconds to wait when --wait is used')
@click.option('--interval', default=3, type=int, help='Polling interval seconds')
@common_output_options
@click.pass_context
@error_handler
def get_job(ctx, job_id, wait, timeout, interval, output, no_cache):
    """Get a job by ID"""
    client, config_manager = ClientManager().get_client_and_config(ctx, cache_namespace='ic')
    if wait:
        data = client.ic_wait_for_job(job_id, timeout=timeout, interval=interval)
    else:
        cache_key = f"job_{job_id}"
        data = None
        if client.cache_manager and not no_cache:
            data = client.cache_manager.get('ic', cache_key)
            if data:
                OutputFormatter.display_cached_message()
        if not data:
            data = client.ic_get_job(job_id)
            if client.cache_manager and not no_cache:
                client.cache_manager.set('ic', cache_key, data)

    def table_fmt(d):
        payload = d.get('data', d)
        jobwrap = payload.get('job', payload) if isinstance(payload, dict) else payload
        job = jobwrap.get('job', jobwrap) if isinstance(jobwrap, dict) else jobwrap
        table = OutputFormatter.create_standard_table(f"Job {job.get('jobId','')}", [
            {'name': 'Field', 'style': 'cyan'},
            {'name': 'Value', 'style': 'white'},
        ])
        for field in ['name', 'status', 'duration', 'startedAt', 'endedAt', 'workflowVersionId', 'owner']:
            table.add_row(field, str(job.get(field, '')))
        console.print(table)

    OutputFormatter.output_data(data, output, config_manager, table_formatter=table_fmt)


# -----------------
# Global Artifacts
# -----------------
@ic_group.group(name='ga')
def ga_group():
    """Manage InsightConnect Global Artifacts"""
    pass


@ga_group.command(name='list')
@click.option('--limit', default=30, type=int)
@click.option('--offset', default=0, type=int)
@click.option('--name', type=str, help='Filter by name')
@click.option('--tag', 'tags', multiple=True, help='Filter by tag (repeatable)')
@common_output_options
@click.pass_context
@error_handler
def ga_list(ctx, limit, offset, name, tags, output, no_cache):
    client, config_manager = ClientManager().get_client_and_config(ctx, cache_namespace='ic')
    cache_key = f"ga_list_{limit}_{offset}_{name}_{','.join(tags) if tags else ''}"
    def fetch():
        return client.ic_list_global_artifacts(limit=limit, offset=offset, name=name, tags=list(tags) if tags else None)

    data = None
    if client.cache_manager and not no_cache:
        data = client.cache_manager.get('ic', cache_key)
        if data:
            OutputFormatter.display_cached_message()
    if not data:
        data = fetch()
        if client.cache_manager and not no_cache:
            client.cache_manager.set('ic', cache_key, data)

    def table_fmt(d):
        payload = d.get('data', d)
        items = payload.get('globalArtifacts', payload) if isinstance(payload, dict) else payload
        if not items:
            console.print('No global artifacts found', style='yellow')
            return
        table = OutputFormatter.create_standard_table('Global Artifacts', [
            {'name': 'ID', 'style': 'cyan'},
            {'name': 'Name', 'style': 'white'},
            {'name': 'Tags', 'style': 'green'},
            {'name': 'Entities', 'style': 'yellow'},
        ])
        for ga in items:
            table.add_row(
                str(ga.get('id', '')),
                str(ga.get('name', '')),
                ", ".join(ga.get('tags', []) or []),
                str(ga.get('entitiesCount', '')),
            )
        console.print(table)

    OutputFormatter.output_data(data, output, config_manager, table_formatter=table_fmt)


@ga_group.command(name='get')
@click.argument('artifact_id')
@common_output_options
@click.pass_context
@error_handler
def ga_get(ctx, artifact_id, output, no_cache):
    client, config_manager = ClientManager().get_client_and_config(ctx, cache_namespace='ic')
    cache_key = f"ga_get_{artifact_id}"
    data = None
    if client.cache_manager and not no_cache:
        data = client.cache_manager.get('ic', cache_key)
        if data:
            OutputFormatter.display_cached_message()
    if not data:
        data = client.ic_get_global_artifact(artifact_id)
        if client.cache_manager and not no_cache:
            client.cache_manager.set('ic', cache_key, data)

    def table_fmt(d):
        payload = d.get('data', d)
        ga = payload.get('globalArtifact', payload) if isinstance(payload, dict) else payload
        table = OutputFormatter.create_standard_table(f"Global Artifact {artifact_id}", [
            {'name': 'Field', 'style': 'cyan'},
            {'name': 'Value', 'style': 'white'},
        ])
        
        # Basic info
        table.add_row('ID', str(ga.get('id', '')))
        table.add_row('Name', str(ga.get('name', '')))
        table.add_row('Description', str(ga.get('description') or 'None'))
        table.add_row('Tags', ", ".join(ga.get('tags', [])) if ga.get('tags') else 'None')
        table.add_row('Entities Count', str(ga.get('entitiesCount', '')))
        table.add_row('Entities Limit', str(ga.get('entitiesLimit', '')))
        table.add_row('Created At', str(ga.get('createdAt', '')))
        table.add_row('Updated At', str(ga.get('updatedAt', '')))
        
        # Active workflows
        workflows = ga.get('activeWorkflowVersions', [])
        if workflows:
            workflow_names = [wf.get('name', '') for wf in workflows]
            table.add_row('Active Workflows', ", ".join(workflow_names[:3]) + ('...' if len(workflow_names) > 3 else ''))
        
        console.print(table)

    OutputFormatter.output_data(data, output, config_manager, table_formatter=table_fmt)


@ga_group.command(name='create')
@click.option('--name', required=True, help='Artifact name')
@click.option('--description', default='', help='Description')
@click.option('--tag', 'tags', multiple=True, help='Tags')
@common_output_options
@click.pass_context
@error_handler
def ga_create(ctx, name, description, tags, output, no_cache):
    client, config_manager = ClientManager().get_client_and_config(ctx, cache_namespace='ic')
    data = client.ic_create_global_artifact(name=name, description=description, schema=None, tags=list(tags) if tags else None)
    
    def table_fmt(d):
        table = OutputFormatter.create_standard_table("Global Artifact Created", [
            {'name': 'Field', 'style': 'cyan'},
            {'name': 'Value', 'style': 'white'},
        ])
        ga = d.get('data', {}).get('globalArtifact', d) if isinstance(d, dict) else {}
        table.add_row('ID', str(ga.get('id', '')))
        table.add_row('Name', str(name))
        table.add_row('Description', str(description or 'None'))
        table.add_row('Tags', ', '.join(tags) if tags else 'None')
        table.add_row('Status', 'Created Successfully')
        console.print(table)
    
    OutputFormatter.output_data(data, output, config_manager, table_formatter=table_fmt)


@ga_group.command(name='delete')
@click.argument('artifact_id')
@common_output_options
@click.pass_context
@error_handler
def ga_delete(ctx, artifact_id, output, no_cache):
    client, config_manager = ClientManager().get_client_and_config(ctx, cache_namespace='ic')
    data = client.ic_delete_global_artifact(artifact_id)
    
    def table_fmt(d):
        table = OutputFormatter.create_standard_table("Global Artifact Deletion", [
            {'name': 'Field', 'style': 'cyan'},
            {'name': 'Value', 'style': 'white'},
        ])
        table.add_row('Artifact ID', str(artifact_id))
        table.add_row('Status', 'Deleted Successfully')
        if isinstance(d, dict) and d.get('message'):
            table.add_row('Message', str(d.get('message')))
        console.print(table)
    
    OutputFormatter.output_data(data, output, config_manager, table_formatter=table_fmt)


@ga_group.group(name='entities')
def ga_entities_group():
    """Manage Global Artifact Entities"""
    pass


@ga_entities_group.command(name='list')
@click.argument('artifact_id')
@click.option('--limit', default=30, type=int)
@click.option('--offset', default=0, type=int)
@common_output_options
@click.pass_context
@error_handler
def ga_entities_list(ctx, artifact_id, limit, offset, output, no_cache):
    client, config_manager = ClientManager().get_client_and_config(ctx, cache_namespace='ic')
    cache_key = f"ga_entities_{artifact_id}_{limit}_{offset}"
    data = None
    if client.cache_manager and not no_cache:
        data = client.cache_manager.get('ic', cache_key)
        if data:
            OutputFormatter.display_cached_message()
    if not data:
        data = client.ic_list_global_artifact_entities(artifact_id, limit=limit, offset=offset)
        if client.cache_manager and not no_cache:
            client.cache_manager.set('ic', cache_key, data)

    def table_fmt(d):
        payload = d.get('data', d)
        items = payload.get('entities', payload) if isinstance(payload, dict) else payload
        if not items:
            console.print('No entities found', style='yellow')
            return
        table = OutputFormatter.create_standard_table('Global Artifact Entities', [
            {'name': 'ID', 'style': 'cyan'},
            {'name': 'Data', 'style': 'white'},
            {'name': 'Updated At', 'style': 'yellow'},
        ])
        for e in items:
            table.add_row(str(e.get('id','')), str(e.get('data','')), str(e.get('updatedAt','')))
        console.print(table)

    OutputFormatter.output_data(data, output, config_manager, table_formatter=table_fmt)


@ga_entities_group.command(name='add')
@click.argument('artifact_id')
@click.option('--data', required=True, help='Entity data (string)')
@common_output_options
@click.pass_context
@error_handler
def ga_entities_add(ctx, artifact_id, data, output, no_cache):
    client, config_manager = ClientManager().get_client_and_config(ctx, cache_namespace='ic')
    resp = client.ic_add_global_artifact_entity(artifact_id, data)
    
    def table_fmt(d):
        table = OutputFormatter.create_standard_table("Global Artifact Entity Added", [
            {'name': 'Field', 'style': 'cyan'},
            {'name': 'Value', 'style': 'white'},
        ])
        table.add_row('Artifact ID', str(artifact_id))
        table.add_row('Entity Data', str(data)[:100] + ('...' if len(str(data)) > 100 else ''))
        entity = d.get('data', {}).get('entity', d) if isinstance(d, dict) else {}
        if entity.get('id'):
            table.add_row('Entity ID', str(entity.get('id')))
        table.add_row('Status', 'Added Successfully')
        console.print(table)
    
    OutputFormatter.output_data(resp, output, config_manager, table_formatter=table_fmt)


@ga_entities_group.command(name='delete')
@click.argument('artifact_id')
@click.argument('entity_id')
@common_output_options
@click.pass_context
@error_handler
def ga_entities_delete(ctx, artifact_id, entity_id, output, no_cache):
    client, config_manager = ClientManager().get_client_and_config(ctx, cache_namespace='ic')
    resp = client.ic_delete_global_artifact_entity(artifact_id, entity_id)
    
    def table_fmt(d):
        table = OutputFormatter.create_standard_table("Global Artifact Entity Deletion", [
            {'name': 'Field', 'style': 'cyan'},
            {'name': 'Value', 'style': 'white'},
        ])
        table.add_row('Artifact ID', str(artifact_id))
        table.add_row('Entity ID', str(entity_id))
        table.add_row('Status', 'Deleted Successfully')
        if isinstance(d, dict) and d.get('message'):
            table.add_row('Message', str(d.get('message')))
        console.print(table)
    
    OutputFormatter.output_data(resp, output, config_manager, table_formatter=table_fmt)
