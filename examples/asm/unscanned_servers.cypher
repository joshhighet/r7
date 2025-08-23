// Unscanned Workstations and Servers
// Find workstations and servers without vulnerability scanning mitigation

MATCH (m:Machine) 
WHERE NOT 'Vulnerability Scanning' IN m.mitigations 
  AND (m.asset_class = 'Workstation' OR m.asset_class = 'Server') 
RETURN m.name, 
       m.asset_class, 
       m.operating_system, 
       m.ips[0] as primary_ip, 
       m.sources as sources, 
       m.mitigations as current_mitigations 
LIMIT 50