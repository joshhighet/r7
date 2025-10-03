// health - Connector Executions
MATCH (e)
WHERE labels(e)[0] = 'sys.apps.workflow-execution'
WITH e, substring(e['sys.apps.workflow-execution:display_id'], 0, 36) AS profile_id
MATCH (p)
WHERE labels(p)[0] = 'sys.apps.profile'
  AND p['sys.apps.profile:id'] = profile_id
  AND p['sys.apps.profile:integration_id'] <> 'rapid7.command_platform.app'
  AND p['sys.apps.profile:integration_id'] <> 'combined.vuln.app'
  AND p['sys.apps.profile:integration_id'] <> 'rdap.app'
  AND p['sys.apps.profile:integration_id'] <> 'noetic.builtins.app'
  AND p['sys.apps.profile:integration_id'] <> 'noetic.dashboard.app'
  AND p['sys.apps.profile:integration_id'] <> 'noetic.ml.app'
  AND p['sys.apps.profile:integration_id'] <> 'nist.nvd.app'
  AND p['sys.apps.profile:integration_id'] <> 'mitre.cwe.app'
  AND p['sys.apps.profile:integration_id'] <> 'mitre.attack.app'
  AND p['sys.apps.profile:integration_id'] <> 'first.epss.app'
  AND p['sys.apps.profile:integration_id'] <> 'cisa.exploit.app'
RETURN p['sys.apps.profile:name'] AS profile_name,
       p['sys.apps.profile:integration_id'] AS app_id,
       e['sys.apps.workflow-execution:status'] AS status,
       e['sys.apps.workflow-execution:status_message'] AS status_message,
       e['samos:modified'] AS timestamp,
       e['sys.apps.workflow-execution:id'] AS exec_id
ORDER BY timestamp DESC
LIMIT 500
