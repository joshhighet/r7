// Ten Machines
// List 10 machines in the environment
// Columns: [{"alias": "m", "property_name": "name"}]

MATCH (m:Machine) 
RETURN m 
LIMIT 10