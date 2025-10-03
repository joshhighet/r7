// health - Connector Profiles
MATCH (i)
WHERE labels(i)[0] = 'sys.apps.integration'
WITH i['sys.apps.integration:id'] AS app_id,
     i['sys.apps.integration:name'] AS app_name
OPTIONAL MATCH (p)
WHERE labels(p)[0] = 'sys.apps.profile'
  AND p['sys.apps.profile:integration_id'] = app_id
WITH app_id, app_name, count(p) AS profile_count
WHERE app_id <> 'rapid7.command_platform.app'
  AND app_id <> 'combined.vuln.app'
  AND app_id <> 'noetic.builtins.app'
  AND app_id <> 'noetic.dashboard.app'
  AND app_id <> 'noetic.ml.app'
  AND app_id <> 'nist.nvd.app'
  AND app_id <> 'mitre.cwe.app'
  AND app_id <> 'mitre.attack.app'
  AND app_id <> 'first.epss.app'
  AND app_id <> 'cisa.exploit.app'
RETURN app_name, app_id, profile_count
ORDER BY profile_count DESC, app_name
