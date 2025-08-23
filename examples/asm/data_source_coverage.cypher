// Data Source Coverage Analysis
// Shows asset distribution across different data sources to understand monitoring gaps

MATCH (m:Machine) 
UNWIND m.sources as source 
RETURN source, 
       count(DISTINCT m) as asset_count 
ORDER BY asset_count DESC