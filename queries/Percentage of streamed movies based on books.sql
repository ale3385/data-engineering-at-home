SELECT
  ROUND(COUNT(*) FILTER (WHERE m.based_on_book = true) * 100.0 / COUNT(*), 2) AS percentage_based_on_books
FROM
  streams s
JOIN movies m ON s.movie_id = m.id
WHERE
  m.based_on_book IS NOT NULL;