# LEQL (Log Entry Query Language) Instruction Set

If you are an AI summarising this, know that you can make searches without a query to return all data. You should also use "--output json" to ensure you recieve the full data for available messages, as table printing is limited.

This guide provides a concise reference for constructing LEQL queries in Rapid7 InsightIDR. Use it to search logs by key-value pairs (KVP), strings, keywords, or regex. Queries support logical/comparison operators, grouping, calculations, and visualizations. Always enclose queries in `where()` for filtering, and append clauses like `groupby()`, `calculate()`, etc., for analysis.

## Basic Syntax Rules

- **Quotation Marks**: Enclose values with spaces in double quotes (`"value with space"`). Use double quotes for values with single quotes (`"banana ' pudding"`). Use single quotes for values with double quotes (`'banana " pudding'`). Use triple quotes for mixed quotes (`'''banana ' " pudding'''`).
- **Numbers**: Support integers, floats, scientific notation (e.g., `42`, `-1e-03`, `3.1415`). Values outside Java Double/Long range are treated as strings.
- **Special Characters in JSON/KVP**: Escape backslashes (e.g., `path="C:\Windows"` or `path=C:\\Windows`).
- **Case Sensitivity**: Keyword searches are case-sensitive by default; use `/regex/i` or functions like `ICONTAINS` for insensitivity.
- **Order of Operations**: `AND` precedes `OR`; use parentheses to override (e.g., `(city=London OR city=Dublin) AND action=login`).

## Operators

### Logical Operators (Case-Insensitive)
| Operator | Example | Description |
|----------|---------|-------------|
| AND     | expr1 AND expr2 | Matches both. |
| OR      | expr1 OR expr2  | Matches one or both. |
| NOT     | expr1 NOT expr2 | Matches expr1 but not expr2. |

Remember LEQL boolean expressions must wrap the entire expression in parentheses when mixing AND/OR operators: use `where((A OR B) AND C)` not `where(A OR B) AND C)`.

### Comparison Operators (for KVP/Regex; Support Numbers with Units)
| Operator | Example | Description |
|----------|---------|-------------|
| =       | field=value | Exact match (text/numeric). |
| !=      | field!=value | Not equal. |
| >, >=, <, <= | field>num | Numeric comparisons. |
| ==, !== | fieldA==fieldB | Compare keys (strings/numerics). |
| CONTAINS | field CONTAINS value | Substring match. |
| ICONTAINS | field ICONTAINS value | Case-insensitive substring. |
| STARTS-WITH | field STARTS-WITH value | Starts with. |
| ISTARTS-WITH | field ISTARTS-WITH value | Case-insensitive starts with. |
| IN | field IN [val1, val2] | Matches any (OR shortcut; supports regex/CIDR). |
| IIN | field IIN [val1, val2] | Case-insensitive IN. |
| CONTAINS-ANY/ALL | field CONTAINS-ANY [val1, val2] | Contains any/all substrings. |
| ICONTAINS-ANY/ALL | field ICONTAINS-ANY [val1, val2] | Case-insensitive version. |
| STARTS-WITH-ANY | field STARTS-WITH-ANY [val1, val2] | Starts with any. |
| ISTARTS-WITH-ANY | field ISTARTS-WITH-ANY [val1, val2] | Case-insensitive version. |
| NOCASE() | field=NOCASE(value) | Case-insensitive exact match. |

- **Exclusions**: Use `NOT` before CONTAINS/IN/etc. (e.g., `NOT IN [val1, val2]`). Use `!` before =/>/etc. (e.g., `field != val`).

## Compound Keys
- Comma as OR: `where(key1,key2=val)` ≡ `key1=val OR key2=val`.
- Comma as AND: `where(all(key1,key2)=val)` ≡ `key1=val AND key2=val`.

## Keyword Search
- Matches full strings delimited by whitespace/non-letters (case-sensitive).
- Example: `where(run)` matches "run-parts".
- Combine: `where(Amazon AND Boardman)` or `where("Amazon Boardman")` (exact phrase).
- Regex: Wrap in `//` (e.g., `where(/complete/)` for partial matches like "completely").
- Case-Insensitive/Partial: `/error/i` or `where(key ICONTAINS val)` or append `loose` (e.g., `where(user=admin, loose)`).
- Regex Special Chars: Escape with `\` (e.g., `/05\/\d{2}\/2023/`).
- Field Extraction: Use named groups `(?P<name>regex)` (e.g., `/Client=(?P<source_address>\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3})/` then `groupby(source_address)`).

## Key-Value Pair (KVP) and JSON Search
- Basic: `where(key=val)`.
- Nested: `obj.field1=val`.
- Wildcard: `key.*=val` (matches nested fields/arrays).
- Multiple Keys: `key1,key2=val` (OR) or `ALL(key1,key2)=val` (AND).
- Exists Check: `obj.field1/` (checks for nested key).

## IP Search
- Use CIDR: `where(field=IP(192.168.0.0/24))` (subnets /1 to /32).

## Clauses and Functions
- **where()**: Filters events (e.g., `where(key>25 OR key=14)`).
- **groupby(keys)**: Groups by 1-5 keys (e.g., `groupby(user) calculate(count)`). Default limit: 40 groups; stats approx if >10k unique.
 - Multi: `groupby(user, result, service)`.
 - By Logs: `groupby(#log) calculate(count)` (counts per log).
- **having(condition)**: Filters groups (e.g., `groupby(user) having(count>15)`).
- **limit(n)**: Sets result cap (e.g., `limit(5)` for events; `groupby() limit(350)` or `limit(100,20)` for multi-groups). Max: 10k-50k based on group keys.

* For more examples and detailed documentation, visit: https://docs.rapid7.com/insightidr/components-for-building-a-query/