SELECT
  COUNT(DISTINCT user_id) AS users_watched_at_least_half
FROM
  streams s
JOIN movies m ON s.movie_id = m.id
WHERE
  EXTRACT(EPOCH FROM (s.end_at - s.start_at)) >= 0.5 * m.duration_mins * 60
  AND s.start_at >= date_trunc('month', current_date) + interval '3 weeks'
  AND s.start_at < date_trunc('month', current_date) + interval '1 month';