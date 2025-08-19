# Rapid7 CLI Quick Guide

A concise guide to using the r7 CLI for security operations across InsightIDR, InsightVM, and Surface Command.

## Getting Started

### Basic Configuration
```bash
# Set up API credentials
r7 config cred store --api-key YOUR_API_KEY

# Set region (us, eu, ca, au, ap)
r7 config set --region us

# Test configuration
r7 account orgs list
```

### View Your Environment
```bash
# See available products
r7 account products list

# Check organization structure
r7 account orgs list
```

## SIEM Operations (InsightIDR)

### Basic Log Exploration
```bash
# View logs overview (logsets, usage, and structure)
r7 siem logs overview

# See what fields are available in a log
r7 siem logs topkeys <LOG_ID> --limit 20
```

### Querying Logs
```bash
# Query specific log source
r7 siem logs query <LOG_ID> "limit(10)" --time-range "Last 4 hours"

# Search across all logs
r7 siem logs query-all "where(source_ip contains '192.168')" --time-range "Last 2 hours"

# Get raw JSON data for analysis
r7 siem logs query <LOG_ID> "limit(5)" --output json
```

### Common LEQL Patterns
```bash
# Authentication failures
r7 siem logs query-all "where(authentication_result=FAILURE)" --time-range "Last 6 hours"

# Group by user activity
r7 siem logs query-all "where(source_user exists) groupby(source_user)" --time-range "Last 12 hours"

# Network activity analysis
r7 siem logs query-all "where(destination_port=443) groupby(destination_ip)" --time-range "Last 1 hour"
```

### Investigations & Alerts
```bash
# List recent investigations
r7 siem investigation list --limit 10

# Get investigation details
r7 siem investigation get <INVESTIGATION_ID>

# View alerts for an investigation
r7 siem investigation alerts <INVESTIGATION_ID>

# Get detailed alert information
r7 siem alert get <ALERT_ID>
```

## Attack Surface Management (ASM)

### Asset Discovery
```bash
# Count discovered assets
r7 asm cypher query "MATCH (m:Machine) RETURN count(m)"
r7 asm cypher query "MATCH (u:User) RETURN count(u)"
r7 asm cypher query "MATCH (d:Domain) RETURN count(d)"

# Find specific assets
r7 asm cypher query "MATCH (m:Machine) WHERE m.ip_address = '192.168.1.100' RETURN m"

# List domains
r7 asm cypher query "MATCH (d:Domain) RETURN d.name LIMIT 10"
```

### Data Connectors
```bash
# View active applications and connectors
r7 asm apps --all-types
```

## Vulnerability Management (InsightVM)

### Configuration Check
```bash
# Test VM console connectivity
r7 vm config-test

# Configure if needed
r7 config cred vm set-user --username user@example.com
r7 config cred vm set-password
r7 config set --vm-console-url https://console.local:3780/api/3
```

### Asset Management
```bash
# List scanning sites
r7 vm sites list --size 10

# View recent scans
r7 vm scans list --size 10

# Search for assets
r7 vm assets search 192.168.1.100

# Get asset details
r7 vm assets get <ASSET_ID>
```

### Vulnerability Analysis
```bash
# Find vulnerabilities for an asset
r7 vm findings asset <ASSET_ID> --size 20 --details

# Get vulnerability details
r7 vm vulns get <VULN_ID>
```

## Common Workflows

### Security Incident Response
1. **Check Recent Activity**
   ```bash
   r7 siem investigation list --limit 5
   ```

2. **Investigate Specific IP**
   ```bash
   r7 siem logs query-all "where(source_ip='192.168.1.100')" --time-range "Last 4 hours"
   r7 asm cypher query "MATCH (m:Machine) WHERE m.ip_address = '192.168.1.100' RETURN m"
   ```

3. **User Activity Analysis**
   ```bash
   r7 siem logs query-all "where(source_user='suspicious_user')" --time-range "Last 24 hours"
   ```

### Threat Hunting
```bash
# Suspicious processes
r7 siem logs query-all "where(process contains 'powershell') groupby(process)" --time-range "Last 2 hours"

# Unusual network connections
r7 siem logs query-all "where(destination_port in [4444, 8080]) groupby(destination_ip)" --time-range "Last 4 hours"

# File access patterns
r7 siem logs query-all "where(filename contains '.exe') limit(50)" --time-range "Last 1 hour"
```

### Asset Correlation
```bash
# Find asset in VM
r7 vm assets search <IP_ADDRESS>

# Check ASM data
r7 asm cypher query "MATCH (m:Machine) WHERE m.ip_address = '<IP_ADDRESS>' RETURN m"

# Search SIEM logs
r7 siem logs query-all "where(source_ip='<IP_ADDRESS>')" --time-range "Last 6 hours"
```

## Performance Tips

- **Time Ranges**: Use specific, short time ranges (≤24 hours) for large queries
- **Query Limits**: Always use `limit(N)` when sampling data
- **Caching**: Commands cache automatically; use `--no-cache` only for fresh data
- **JSON Output**: Use `--output json | jq` for complex filtering and analysis
- **Specific Logs**: Query specific log sources when possible instead of `query-all`

## Troubleshooting

### VM Connection Issues
```bash
r7 vm config-test
```

### SIEM Query Timeouts
- Reduce time range: `--time-range "Last 2 hours"`
- Add query limits: `"your_query limit(100)"`
- Use specific logs instead of `query-all`

### Authentication Errors
```bash
r7 config show
r7 config cred store --api-key NEW_KEY
```

## Getting Help

```bash
# Command help
r7 --help
r7 siem --help
r7 siem logs --help

# LEQL reference
r7 siem logs leql

# Example queries
r7 siem logs examples
```