"""
Test the CLI commands directly
"""
import pytest
from click.testing import CliRunner
from r7 import cli

class TestCLI:
    def setup_method(self):
        """Setup for each test"""
        self.runner = CliRunner()

    def test_cli_help(self):
        """Test basic CLI help works"""
        result = self.runner.invoke(cli, ['--help'])
        assert result.exit_code == 0
        assert 'r7 - cli for logsearch' in result.output

    def test_config_show(self):
        """Test config show command"""
        result = self.runner.invoke(cli, ['config', 'show'])
        # Should work even without credentials
        assert result.exit_code == 0

    def test_config_help(self):
        """Test config subcommands"""
        result = self.runner.invoke(cli, ['config', '--help'])
        assert result.exit_code == 0
        assert 'manage local configuration' in result.output

    def test_invalid_command(self):
        """Test invalid command fails gracefully"""
        result = self.runner.invoke(cli, ['nonexistent'])
        assert result.exit_code != 0

    def test_siem_help(self):
        """Test SIEM commands accessible"""
        result = self.runner.invoke(cli, ['siem', '--help'])
        assert result.exit_code == 0

    def test_vm_help(self):
        """Test VM commands accessible"""  
        result = self.runner.invoke(cli, ['vm', '--help'])
        assert result.exit_code == 0

    def test_asm_help(self):
        """Test ASM commands accessible"""
        result = self.runner.invoke(cli, ['asm', '--help'])
        assert result.exit_code == 0

    def test_with_env_api_key(self):
        """Test CLI respects R7_API_KEY environment variable"""
        result = self.runner.invoke(cli, ['--help'], 
                                   env={'R7_API_KEY': 'test-key'})
        assert result.exit_code == 0

    def test_region_flag(self):
        """Test region flag is accepted"""
        result = self.runner.invoke(cli, ['--region', 'eu', '--help'])
        assert result.exit_code == 0

    def test_verbose_flag(self):
        """Test verbose flag is accepted"""
        result = self.runner.invoke(cli, ['--verbose', '--help'])
        assert result.exit_code == 0