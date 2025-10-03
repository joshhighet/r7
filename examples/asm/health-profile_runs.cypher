// health - Profile Runs
MATCH (e)
WHERE labels(e)[0] = 'sys.apps.workflow-execution'
WITH substring(e['sys.apps.workflow-execution:display_id'], 0, 36) AS 
profile_id,
     max(e['samos:modified']) AS latest_time
MATCH (e2)
WHERE labels(e2)[0] = 'sys.apps.workflow-execution'
  AND e2['samos:modified'] = latest_time
MATCH (p)
WHERE labels(p)[0] = 'sys.apps.profile'
  AND p['sys.apps.profile:id'] = profile_id
RETURN p['sys.apps.profile:name'] AS profile_name,
       p['sys.apps.profile:integration_id'] AS app_id,
       e2['sys.apps.workflow-execution:status'] AS status,
       e2['samos:modified'] AS last_run,
       e2['sys.apps.workflow-execution:id'] AS exec_id
ORDER BY
  CASE e2['sys.apps.workflow-execution:status']
    WHEN 'running' THEN 0
    WHEN 'error' THEN 1
    WHEN 'failed' THEN 1
    WHEN 'timeout' THEN 1
    ELSE 2
  END,
  last_run DESC