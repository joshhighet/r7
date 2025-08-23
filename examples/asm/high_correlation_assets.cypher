// High Correlation Assets
// Find assets visible in multiple data sources for validation and prioritization
// Columns: [{"alias": "m", "property_name": "name"}]

MATCH (m:Machine) 
WHERE size(m.sources) >= 3 
RETURN m.name, 
       m.sources, 
       size(m.sources) as source_count 
ORDER BY source_count DESC