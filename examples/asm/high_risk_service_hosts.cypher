// High Risk Service Hosts
// Find hosts with potentially dangerous services like RDP, VNC, and databases

MATCH (s:RumbleService) 
WHERE s.service_port IN [3389, 5900, 1433, 3306, 5432, 1521, 27017] 
OPTIONAL MATCH (m:Machine) 
WHERE s.service_address IN m.ips 
RETURN s.service_port, 
       s.service_address, 
       m.name, 
       s.service_summary