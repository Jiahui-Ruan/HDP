CREATE VIEW IF NOT EXISTS highestRatingMovies AS
SELECT movieID, avg(rating) AS movieRating
FROM ratings
GROUP BY movieID
ORDER BY movieRating DESC;

SELECT n.title, movieRating
FROM highestRatingMovies h JOIN names n on h.movieID = n.movieID;
