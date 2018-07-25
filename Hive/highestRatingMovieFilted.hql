CREATE VIEW IF NOT EXISTS highestRatingMovies AS
SELECT movieID, AVG(rating) AS movieRating, COUNT(rating) AS ratingCount
FROM ratings
GROUP BY movieID
ORDER BY movieRating DESC;

SELECT n.title, movieRating
FROM highestRatingMovies h JOIN names n on h.movieID = n.movieID
WHERE ratingCount > 10;
