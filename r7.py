#!/usr/bin/env python3
"""
r7 CLI
"""
import click
import logging
from commands.config_commands import config_group
from commands.account_commands import account_group
from commands.appsec_commands import appsec_group
# logs_group now moved to siem.logs
from commands.asm_commands import asm_group
from commands.docs_commands import docs
from commands.idr_commands import siem_group
from commands.vm_commands import vm_group
from commands.ic_commands import ic_group

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

@click.group()
@click.option('--api-key', envvar='R7_API_KEY', help='Rapid7 API Key (or use keychain/env)')
@click.option('--region', type=click.Choice(['us', 'eu', 'ca', 'ap', 'au']), help='API Region')
@click.option('--org-id', help='Organization ID for RRN reconstruction')
@click.option('--verbose', is_flag=True, help='Enable verbose logging')
@click.pass_context
def cli(ctx, api_key, region, org_id, verbose):
    """
    r7 - cli for logsearch, asm, web / net vulns on Rapid7
    """
    if verbose:
        logger.setLevel(logging.DEBUG)
        logging.getLogger('utils').setLevel(logging.DEBUG)
        logging.getLogger('api').setLevel(logging.DEBUG)
        logging.getLogger('commands').setLevel(logging.DEBUG)
    ctx.ensure_object(dict)
    ctx.obj['api_key'] = api_key
    ctx.obj['region'] = region
    ctx.obj['org_id'] = org_id
    ctx.obj['verbose'] = verbose

# Register command groups (nested commands are attached within their modules)
cli.add_command(config_group)
cli.add_command(account_group)
cli.add_command(appsec_group)
# logs_group now part of siem group
cli.add_command(asm_group)
cli.add_command(docs)
cli.add_command(siem_group)
cli.add_command(vm_group)
cli.add_command(ic_group)

if __name__ == '__main__':
    cli(prog_name='r7')