// Critical Vulnerabilities
// Find vulnerabilities with CVSS score >= 9.0 for urgent patching prioritization

MATCH (v:Rapid7IVMVulnerability) 
WHERE v.cvssV3Score >= 9.0 
RETURN v.name, 
       v.cvssV3Score, 
       v.datePublished 
ORDER BY v.cvssV3Score DESC