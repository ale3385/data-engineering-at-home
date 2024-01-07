SELECT
  COUNT(DISTINCT user_id) AS affected_users
FROM
  streams
WHERE
  start_at <= 'YYYY-MM-DD HH:MM:SS' -- Outage start time
  AND end_at >= 'YYYY-MM-DD HH:MM:SS'; -- Outage end time
  ;