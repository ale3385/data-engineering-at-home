SELECT
  AVG(EXTRACT(EPOCH FROM (end_at - start_at))/60) AS avg_streaming_duration_minutes
FROM
  streams;