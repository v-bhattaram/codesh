```
SELECT 
    pid,
    user_name,
    db,
    start_time,
    remotehost,
    port,
    state,
    elapsed,
    queue_start_time,
    substring(query, 1, 100) AS query_snippet
FROM stv_sessions
ORDER BY start_time DESC;
```
```
SELECT 
    userid,
    user_name,
    starttime,
    endtime,
    label,
    substring(querytxt, 1, 100) AS query_snippet,
    aborted
FROM svl_qlog
WHERE starttime >= getdate() - interval '1 hour'
ORDER BY starttime DESC;
```

```
SELECT 
    usename,
    application_name,
    client_addr,
    backend_start,
    state,
    query
FROM pg_catalog.pg_stat_activity
WHERE state = 'active';
```

```
CANCEL SESSION <pid>;
CANCEL <pid>;
```
