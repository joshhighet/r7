import click
from utils.credentials import CredentialManager
from utils.exceptions import AuthenticationError
from utils.config import ConfigManager, ConfigurationError

@click.group(name='cred')
def cred_group():
    """Manage API credentials in macOS Keychain"""
    pass

@cred_group.command()
@click.option('--api-key', required=True, help='API key to store')
def store(api_key):
    """Store API key in macOS Keychain"""
    try:
        if not CredentialManager.validate_api_key(api_key):
            click.echo("Warning: API key appears to be invalid (too short)")
            if not click.confirm("Store anyway?"):
                return
        CredentialManager.store_api_key(api_key)
        click.echo("✅ API key stored successfully in macOS Keychain")
        click.echo("You can now run commands without --api-key or R7_API_KEY")
    except AuthenticationError as e:
        click.echo(f"❌ Error: {e}", err=True)

@cred_group.command()
def delete():
    """Delete API key from macOS Keychain"""
    try:
        if CredentialManager.delete_api_key():
            click.echo("✅ API key deleted from macOS Keychain")
        else:
            click.echo("ℹ️ No API key found in keychain")
    except AuthenticationError as e:
        click.echo(f"❌ Error: {e}", err=True)

@cred_group.command()
def status():
    """Check if API key is stored in keychain"""
    api_key = CredentialManager.get_api_key()
    if api_key:
        masked_key = f"{api_key[:8]}...{api_key[-4:]}" if len(api_key) > 12 else "****"
        source = "keychain" if not click.get_current_context().params.get('api_key') else "provided"
        click.echo(f"✅ API key found ({source}): {masked_key}")
    else:
        click.echo("❌ No API key found in keychain or environment")
        click.echo("Use 'r7 config cred store --api-key YOUR_KEY' to store one")


# --- InsightVM Console credentials ---
@cred_group.group(name='vm')
def vm_cred_group():
    """Manage InsightVM console credentials"""
    pass


@vm_cred_group.command(name='set-user')
@click.option('--username', required=True, help='Console username to store in config')
def vm_set_user(username):
    """Store VM console username in config"""
    try:
        cfg = ConfigManager()
        cfg.set('vm_username', username)
        cfg.save_config()
        click.echo("✅ VM console username saved in config")
    except ConfigurationError as e:
        click.echo(f"❌ Error: {e}", err=True)


@vm_cred_group.command(name='set-password')
@click.option('--password', prompt=True, hide_input=True, confirmation_prompt=True)
def vm_set_password(password):
    """Store VM console password in keychain"""
    try:
        CredentialManager.store_vm_password(password)
        click.echo("✅ VM console password stored in macOS Keychain")
    except AuthenticationError as e:
        click.echo(f"❌ Error: {e}", err=True)


@vm_cred_group.command(name='delete-password')
def vm_delete_password():
    """Delete VM console password from keychain"""
    try:
        if CredentialManager.delete_vm_password():
            click.echo("✅ VM console password deleted from keychain")
        else:
            click.echo("ℹ️ No VM password found in keychain")
    except AuthenticationError as e:
        click.echo(f"❌ Error: {e}", err=True)


@vm_cred_group.command(name='status')
def vm_status():
    """Show stored VM username and whether a password is stored"""
    cfg = ConfigManager()
    user = cfg.get('vm_username')
    pwd = CredentialManager.get_vm_password()
    if user:
        click.echo(f"👤 VM username: {user}")
    else:
        click.echo("👤 VM username: not set")
    click.echo(f"🔐 VM password: {'stored' if pwd else 'not stored'}")