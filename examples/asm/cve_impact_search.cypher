// CVE Impact Search
// Search for a specific CVE and show affected hosts with vulnerability details
// Usage: Replace 'CVE-2024-6387' with the CVE you want to search for
// For detailed view: use as-is
// For host list only: add "MATCH (m:Machine)-->(v)" and "RETURN m.name, m.operating_system, m.ips[0] as primary_ip"

MATCH (v:Rapid7IVMVulnerability) 
WHERE v.name CONTAINS 'CVE-2024-6387'
OPTIONAL MATCH (m:Machine)-->(v) 
RETURN v.name as vulnerability_name,
       v.cvssV3Score as cvss_score,
       v.datePublished as published_date,
       v.hasExploits as has_exploits,
       collect(m.name) as affected_hosts,
       size(collect(m.name)) as total_affected_hosts 
ORDER BY cvss_score DESC