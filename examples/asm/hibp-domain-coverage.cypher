MATCH (d:Domain) 
WHERE d.is_external IS NULL OR d.is_external = False
OPTIONAL MATCH (h:HibpDomain) WHERE toLower(h.name) = toLower(d.name)
RETURN 
     toLower(d.name) as domain_name,
     CASE WHEN h IS NOT NULL THEN 'Covered' ELSE 'Not Covered' END as hibp_status,
     CASE WHEN h IS NOT NULL THEN h.PwnCount ELSE null END as pwn_count,
     CASE WHEN h IS NOT NULL THEN h.NextSubscriptionRenewal ELSE null END as next_renewal
ORDER BY hibp_status ASC, toLower(d.name)