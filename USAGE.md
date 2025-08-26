# General

By default, so long as in an interactive terminal, outputs are in formatted tables. You can always get the full information by using the `--output json` flag. Piping commands, this will trigger an non-interactive output - for example, `r7 account orgs list --output json | jq` only needs to be `r7 account orgs list | jq ` - Certain commands will require the switch `--full-output` - It should rarely be required.

`--verbose`, `--region`, `--org-id` & `--api-key` are global switches. use `--help` for more information, all options should be well documented - highlight any you find are not, or where additional information would have allowed you to solve a task far quicker. Keychain native access is preferred over `--api-key` hence auth shouldn't need to be given by default.

```
Usage: r7 [OPTIONS] COMMAND [ARGS]...

  r7 - cli for logsearch, asm, web / net vulns on Rapid7

Options:
  --api-key TEXT             Rapid7 API Key (or use keychain/env)
  --region [us|eu|ca|ap|au]  API Region
  --org-id TEXT              Organization ID for RRN reconstruction
  --verbose                  Enable verbose logging
  --help                     Show this message and exit.

Commands:
  account  manage users, keys, roles, access etc
  appsec   web app scans, findings
  asm      surface command cypher queries, apps/sdk
  config   manage local configuration
  docs     search docs.rapid7.com content
  ic       manage automation jobs, execute workflows
  siem     search logs, manage alerts/investigations
  vm       core vulnerability mgt, console & cloud
  ```

# SIEM

`logs` is how we interface with the Rapid7 SIEM. This includes searching logs, managing alerts and investigations. There are several helpful notes for when making queries below, however you should always use 'topkeys' when investigating data you haven't worked with before to know which fields are available. You can view a single full log with nothign else like so when required `r7 siem logs query sublime-security 'limit(1)' --output json`

Logs in this SIEM can be queried in three ways - against an individual log, a 'log set' (collection of logs), or everything. By default, in an interactive terminal session, outputs are in formatted tables with commonly sighted keys.

Use `r7 siem logs overview` to get an understanding on the logs and log sets available along with their volume. `r7 siem logs examples` shows a few working searches.
Before making searches, ALWAYS use `r7 siem logs topkeys (id)` to find out the event schema - understand historical data for a collection with `r7 siem logs usage-specific (id)`
To query logs, use `r7 siem logs` with `query`, `query-logset` or `query-all` with an optional LEQL statement.
LEQL is the DSL used by the SIEM and more information can be found on the schema with `r7 siem logs leql` and examples can be seen with `r7 siem logs examples`
You can quickly fetch a subset of events for a log to get an understanding of the data with a LEQL query such as `limit(5)`
By default searches will return only a subset of common fields - you can increase the number of fields available with `--smart-columns-max (int)` or view all with `--no-smart-columns`

To query logs in a specific log set, you can use the `query-logset` command. For example, looking for authentication events; `r7 siem logs query-logset "Asset Authentication" "where(source_asset contains insight-vm AND service contains sudo) limit(10)" --time-range "Last 7 days"` - or against a direct log for for process events from a specific user; `r7 siem logs query "Process Start Events" "where(hostname=windows-desktop AND process.username=josh) limit(10)" --time-range "Last 7 days"` - If I was intereted in a particular user's access to various systems, I would look at `r7 siem logs query-logset "Asset Authentication" "where(source_local_account=josh) groupby(source_asset)" --time-range "Last 7 days"`.

JSON outputs can be large, so use limits to understand the data we are working with, considering limited context windows - the LEQL `limit` should be used when searching raw logs, or the `select` claude once we know what we're lookign for across a large subset of data. Use `--time-range "last 5 hours"` or `yesterday` for relative queries, `--from-date "2024-01-15"` `--to-date "2024-01-20"` for specific date ranges, or `--from-time 1450557004000` `--to-time 1460557604000` for exact timestamps. Only one time method can be used per query - if none specified, defaults to "Last 30 days". i.e `r7 siem logs query-all "limit(10)" --time-range "last 2 hours"` or `r7 siem logs query "Raw Log" "limit(5)" --from-date "2024-12-01 14:00" --to-date "2024-12-01 18:00"`

Alerts are triggered based on specific conditions in log data and investigations are notable roll-ups of related alerts and events. You can list unique alerts with `r7 siem alert list` and retrieve specifics with `r7 siem alert get` on an ID or RRN

You can list investigations with `r7 siem investigation list` and retrieve specifics with `r7 siem investigation get` on an ID or RRN. To view alerts that have made up an investigation, use `r7 siem investigation alerts` on the ID. Use `r7 siem investigation` or `alert` with `--help` for more, such as making comments, assigning cases, creating etc

# ASM

`asm` is how we interface with Surface Command's correlative asset graph with relational connectors to various enterprise toolsets.  To understand which connections have been established and the types they make available to us, run `r7 asm apps`. We leverage openCypher (OC) making queries to AgensGraph (not ANSI-SQL). To query data within the asset graph, use `r7 asm cypher` - `r7 asm cypher examples` has a number of basic examples to show correlative properties.

More information can be found on our OC syntax with `r7 asm cypher docs` and examples can be seen with `r7 asm cypher examples`
A simple asset search may look something like; `r7 asm cypher query 'MATCH (m:Asset) WHERE m.hostnames ISTARTS WITH "multisocks.dark" RETURN m' --columns 'm.name,m.sources,m.hostnames,m.ips'` - Outputs can be pretty verbose, you can process some of this out when you understand the jsonpaths - here's an example: `r7 asm cypher query 'MATCH (m:Machine) RETURN m.name, m.sources, size(m.ips) as ip_count' | jq -r '.items[] | [.data[0], (.data[1] | join(",")), .data[2]] | @tsv'`

For example to get 10 vulns on host windows-desktop we could use `r7 asm cypher query "MATCH (m:Machine)-->(v:Rapid7IVMVulnerability) WHERE m.name = 'windows-desktop' RETURN v LIMIT 10"`. We could look for hosts with docker bridge networks with something like `r7 asm cypher query "MATCH (m:Machine) WHERE any(ip IN m.ips WHERE ip STARTS WITH '172.') RETURN m.name, [ip IN m.ips WHERE ip STARTS WITH '172.'] as docker_ips"`. We could check out assets we have in multiple security tools with `r7 asm cypher query "MATCH (m:Machine) WHERE size(m.sources) >= 3 RETURN m.name, m.sources, size(m.sources) as source_count ORDER BY source_count DESC"` and review the users on endpoints with some level of administrative access with `r7 asm cypher query "MATCH (m:Machine)-->(u:User) WHERE u.is_administrator = true RETURN m.name, u.name"`


# VM

`vm` is how we interface with InsightVM - Rapid7's vulnerability management platform. you can search for assets by hostname like so `r7 vm console assets list --hostname multisocks.dark` and doing a `r7 vm assets get (id)`  and commands like `r7 vm console findings asset 1 --port 22 --details` or `r7 vm console findings asset 1 --status vulnerable --details` for technical details.

# APPSEC

`appsec` allows communication with InsightAppSec. Look at what webapps we have access to with `r7 appsec app list` and check out full details with `r7 appsec app get "my Website"`

# DOCS

`docs` searches the Algolia index for docs.rapid7.com to allow agents to quickly find relevant documentation.

# SOAR

`ic` is how we interface with InsightConnect - Rapid7's security orchestration and automation platform.
You can check out created workflows with `r7 ic workflows list` - other commands allow for invoking/controlling/exporting workflows.
The jobs subcommand shows previous executions of established workflows with filters, for examples `r7 ic jobs list --status failed --limit 5` to show the last five failed runs.
