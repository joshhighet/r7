# Cypher DSL Reference for Rapid7 ASM

This guide provides a reference for the Cypher query language used in ASM. Use this to query the graph for correlative analysis across toolsets.

## Available Functions

## Aggregation Functions
- `count()` - Count items
- `collect()` - Collect items into list
- `min()`, `max()` - Min/max values
- `avg()` - Average of numeric values
- `sum()` - Sum of numeric values

## String Functions
- `CONTAINS`, `STARTS WITH`, `ENDS WITH` - Pattern matching
- `toUpper()`, `toLower()` - Case conversion
- `substring(string, start, length)` - Extract substring
- `length()` - String length
- `split(string, delimiter)` - Split into list
- `replace(string, search, replace)` - Replace text
- `trim()` - Remove whitespace

## Type Conversion
- `toString()` - Convert to string
- `toInteger()` - Convert to integer
- `toFloat()` - Convert to float

## List Functions
- `size()` - List size
- `head()` - First element
- `tail()` - All but first element
- `last()` - Last element
- `range(start, end)` - Generate number range

## List Predicates
- `any(item IN list WHERE condition)` - Any match
- `all(item IN list WHERE condition)` - All match
- `none(item IN list WHERE condition)` - No match
- `single(item IN list WHERE condition)` - Exactly one match

## List Comprehensions
- `[item IN list WHERE condition | expression]` - Transform lists

## Math Functions
- `abs()` - Absolute value
- `round()`, `floor()`, `ceil()` - Rounding
- `sqrt()` - Square root
- `sign()` - Sign of number
- `rand()` - Random number

## Null Handling
- `coalesce(value1, value2, ...)` - First non-null value
- `IS NULL`, `IS NOT NULL` - Null checks

## Metadata Functions
- `labels(node)` - Get node labels
- `keys(node)` - Get property names
- `properties(node)` - Get all properties as map
- `id(node)` - Get internal node ID
- `type(relationship)` - Get relationship type

## Query Clauses
- `ORDER BY`, `SKIP`, `LIMIT` - Result ordering/pagination
- `WITH` - Intermediate processing
- `DISTINCT` - Remove duplicates
- `UNION` - Combine results
- Variable length paths: `[*1..2]`

## Not Available

## Functions
- `exists()` - Property existence check
- `type()` on nodes (only works on relationships)
- `reverse()` on lists (only on strings)
- `timestamp()`, `datetime()`, `date()` - Temporal functions
- Regular expressions: `=~`

## Advanced Features
- `UNWIND` - List expansion (scope issues)
- Complex string operations with mixed types
- Temporal/date operations

## Notable Limitations
- String concatenation with mixed types fails
- Some list operations have type restrictions
- Regular expression matching not supported
- Limited temporal function support

## Most Useful for Incident Response

- **List Predicates** (`any()`, `all()`, `none()`) - Perfect for IP/asset filtering
- **String Functions** (`CONTAINS`, `STARTS WITH`) - Pattern matching for hostnames/domains
- **Aggregation + WITH** - Complex analytics queries
- **List Comprehensions** - Transform and filter data
- **Metadata Functions** (`labels()`, `properties()`) - Schema discovery

## Examples

For practical examples of Cypher queries, use: `r7 asm cypher examples`
