# General

By default, so long as in an interactive terminal, outputs are in formatted tables. You can always get the full information by using the `--output json` flag.

`--verbose`, `--region`, `--org-id` & `--api-key` are global switches. use `--help` for more information, all options should be well documented - highlight any you find are not, or where additional information would have allowed you to solve a task far quicker. Keychain native access is preferred over `--api-key`


# SIEM

`logs` is how we interface with the Rapid7 SIEM. This includes searching logs, managing alerts and investigations.

Alerts are triggered based on specific conditions in log data and investigations are notable roll-ups of related alerts and events.
You can list unique alerts with `r7 siem alert list` and retrieve specifics with `r7 siem alert get` on an ID or RRN
You can list investigations with `r7 siem investigation list` and retrieve specifics with `r7 siem investigation get` on an ID or RRN.
To view alerts that have made up an investigation, use `r7 siem investigation alerts` on the ID.
Use `r7 siem investigation` or `alert` with `--help` for more, such as making comments, assigning cases, creating etc

Logs in this SIEM can be queried in three ways - against an individual log, a 'log set' (collection of logs), or everything.
Use `r7 siem logs overview` to get an understanding on the logs and log sets available along with their volume. `r7 siem logs examples` shows a few working searches.
Logs can be filtered by their values, to find out what's available - use `r7 siem logs topkeys (id)` and understand historical data for a log with `r7 siem logs usage-specific (id)`
To query logs, use `r7 siem logs` with `query`, `query-logset` or `query-all` with an optional LEQL statement.
LEQL is the DSL used by the SIEM and more information can be found on the schema with `r7 siem logs leql` and examples can be seen with `r7 siem logs examples`
You can quickly fetch a subset of events for a log to get an understanding of the data with a LEQL query such as `limit(5)`
By default searches will return only a subset of common fields - you can increase the number of fields available with `--smart-columns-max (int)` or view all with `--no-smart-columns`

# ASM

`asm` is how we interface with Surface Command (CAASM) - A correlative asset graph with relational connectors to various enterprise toolsets.
To understand which connections have been established and the Neo4J types they make available to us, run `r7 asm apps`
To query data within the asset graph, use `r7 asm cypher` - `r7 asm cypher examples` has a number of basic examples to show correlative properties.
A simple asset search may look something like; `r7 asm cypher query 'MATCH (m:Asset) WHERE m.hostnames ISTARTS WITH "multisocks.dark" RETURN m' --columns 'm.name,m.sources,m.hostnames,m.ips'`

# VM

`vm` is how we interface with InsightVM - Rapid7's vulnerability management platform. you can search for assets by hostname like so `r7 vm console assets list --hostname multisocks.dark` and doing a `r7 vm assets get (id)` for further details.

# APPSEC

`appsec` allows communication with InsightAppSec - Rapid7's application security testing solution. 

# DOCS

`docs` searches the Algolia index for docs.rapid7.com to allow agents to quickly find relevant documentation.

# SOAR

`ic` is how we interface with InsightConnect - Rapid7's security orchestration and automation platform.
You can check out created workflows with `r7 ic workflows list` - other commands allow for invoking/controlling/exporting workflows.
The jobs subcommand shows previous executions of established workflows with filters, for examples `r7 ic jobs list --status failed --limit 5` to show the last five failed runs.
