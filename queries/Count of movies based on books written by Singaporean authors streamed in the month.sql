SELECT
  COUNT(DISTINCT s.movie_id) AS singaporean_authors_movie_count
FROM
  streams s
JOIN movies m ON s.movie_id = m.id
JOIN books b ON m.book_id = b.id
JOIN authors a ON b.author_id = a.id
WHERE
  a.nationality = 'Singaporean'
  AND s.stream_date BETWEEN 'YYYY-MM-01' AND 'YYYY-MM-31';