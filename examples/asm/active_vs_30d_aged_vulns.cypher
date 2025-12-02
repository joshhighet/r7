MATCH (a:Rapid7IVMAsset)<--(f1:Rapid7IVMFinding)-->(e1:Rapid7IVMVulnerability)
WHERE e1.severity IN ["Critical", "High"]
WITH a,
     count(CASE WHEN e1.severity = "Critical" THEN 1 END) AS Total_Critical,
     count(CASE WHEN e1.severity = "High"     THEN 1 END) AS Total_High,
     sum(e1.riskScoreV2_0)                                   AS Total_Risk

MATCH (a)<--(f2:Rapid7IVMFinding)-->(e2:Rapid7IVMVulnerability)
WHERE e2.severity IN ["Critical", "High"]
  AND since(f2.first_seen, 'days') > 30

WITH a, Total_Critical, Total_High, Total_Risk,
     count(CASE WHEN e2.severity = "Critical" THEN 1 END) AS Aged_Critical,
     count(CASE WHEN e2.severity = "High"     THEN 1 END) AS Aged_High,
     sum(e2.riskScoreV2_0)                                   AS Aged_Risk,
     collect(e2.vulnId)[0..20]                               AS Aged_VulnIDs

RETURN 
  a.ip            AS IP,
  a.hostName      AS Hostname,
  a.osDescription AS OS,
  Total_Risk,
  Aged_Risk,
  Total_Critical,
  Total_High,
  Aged_High,
  Aged_Critical,
  Aged_VulnIDs

ORDER BY Aged_Critical DESC, Aged_High DESC, Aged_Risk DESC
LIMIT 100
