// Single Source Assets
// Identify assets with limited visibility - potential monitoring blind spots

MATCH (m:Machine) 
WHERE size(m.sources) = 1 
RETURN m.name, 
       m.sources[0] as only_source, 
       m.ips[0] as primary_ip