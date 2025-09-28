// BIMI Domain Analysis
// Analyzes all domains with DMARC records for BIMI eligibility

MATCH (zone:CloudflareDnsZone)
OPTIONAL MATCH (dmarc:CloudflareDnsRecord)
WHERE dmarc.type = 'TXT'
  AND (dmarc.name = '_dmarc.' + zone.name OR dmarc.name = '_dmarc.' + zone.name + '.')
OPTIONAL MATCH (spf:CloudflareDnsRecord)
WHERE spf.name = zone.name
  AND spf.type = 'TXT'
  AND spf.content ISTARTS WITH 'v=spf1'
  AND spf.content ICONTAINS '-all'
WITH zone.name AS domain, dmarc, spf,
     CASE 
       WHEN dmarc IS NULL THEN false
       WHEN NOT dmarc.content =~ '(?i).*pct=.*' THEN true
       WHEN dmarc.content =~ '(?i).*pct=100.*' THEN true
       ELSE false
     END AS dmarc_full_coverage
RETURN domain,
       CASE 
         WHEN dmarc IS NULL THEN 'No DMARC'
         WHEN dmarc.content ICONTAINS 'p=reject' THEN 'reject'
         WHEN dmarc.content ICONTAINS 'p=quarantine' THEN 'quarantine'
         WHEN dmarc.content ICONTAINS 'p=none' THEN 'none'
         ELSE 'unknown'
       END AS dmarc_policy,
       CASE WHEN spf IS NOT NULL THEN 'Yes' ELSE 'No' END AS strict_spf,
       CASE WHEN dmarc_full_coverage THEN 'Yes' ELSE 'No' END AS full_dmarc_coverage,
       CASE
         WHEN dmarc IS NULL THEN 'Not Eligible (No DMARC)'
         WHEN NOT (dmarc.content ICONTAINS 'p=quarantine' OR dmarc.content ICONTAINS 'p=reject') 
           THEN 'Not Eligible (DMARC policy insufficient)'
         WHEN NOT dmarc_full_coverage THEN 'Not Eligible (DMARC coverage <100%)'
         WHEN spf IS NOT NULL AND dmarc.content ICONTAINS 'p=reject' THEN 'Fully Eligible'
         WHEN spf IS NOT NULL AND dmarc.content ICONTAINS 'p=quarantine' THEN 'Eligible'
         ELSE 'Partial (no SPF -all)'
       END AS bimi_eligibility
ORDER BY 
  CASE bimi_eligibility
    WHEN 'Fully Eligible' THEN 1
    WHEN 'Eligible' THEN 2
    WHEN 'Partial (no SPF -all)' THEN 3
    ELSE 4
  END, domain ASC
