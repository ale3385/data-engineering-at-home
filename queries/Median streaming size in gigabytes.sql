SELECT
  PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY size_mb / 1024) AS median_streaming_size_gb
FROM
  streams;