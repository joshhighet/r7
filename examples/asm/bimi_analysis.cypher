// BIMI Domain Analysis
// Analyzes all domains with DMARC records for BIMI eligibility

MATCH (dmarc:CloudflareDnsRecord) 
WHERE dmarc.type = 'TXT' 
  AND dmarc.name ISTARTS WITH '_dmarc.' 
WITH dmarc, REPLACE(dmarc.name, '_dmarc.', '') AS domain 

OPTIONAL MATCH (spf:CloudflareDnsRecord) 
WHERE spf.name = domain 
  AND spf.type = 'TXT' 
  AND spf.content CONTAINS 'v=spf1'

RETURN domain,
       CASE 
         WHEN dmarc.content ICONTAINS 'p=reject' THEN 'reject'
         WHEN dmarc.content ICONTAINS 'p=quarantine' THEN 'quarantine' 
         WHEN dmarc.content ICONTAINS 'p=none' THEN 'none'
         ELSE 'unknown'
       END AS dmarc_policy,
       
       CASE 
         WHEN spf IS NOT NULL AND (spf.content ICONTAINS '-all') THEN 'Yes'
         WHEN spf IS NOT NULL THEN 'Soft fail only' 
         ELSE 'No SPF'
       END AS spf_status,
       
       CASE 
         WHEN spf IS NOT NULL AND (spf.content ICONTAINS '-all') AND dmarc.content ICONTAINS 'p=reject' THEN 'Fully Eligible'
         WHEN spf IS NOT NULL AND (spf.content ICONTAINS '-all') AND dmarc.content ICONTAINS 'p=quarantine' THEN 'Eligible' 
         WHEN dmarc.content ICONTAINS 'p=reject' OR dmarc.content ICONTAINS 'p=quarantine' THEN 'Partial (SPF issues)'
         ELSE 'Not Eligible'
       END AS bimi_eligibility

ORDER BY bimi_eligibility DESC, domain ASC