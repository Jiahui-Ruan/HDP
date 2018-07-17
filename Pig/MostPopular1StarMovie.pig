ratings = LOAD '/user/maria_dev/ml-100k/u.data' AS (userID:int, movieID:int, rating:int, ratingTime:int);

metadata = LOAD '/user/maria_dev/ml-100k/u.item' USING PigStorage('|')
    AS (movieID:int, movieTitle:chararray, releaseDate:chararray, videoRelease:chararray, imdbLink:chararray);

ratingsByMoive = GROUP ratings BY movieID;

avgRatings = FOREACH ratingsByMoive GENERATE group AS movieID, AVG(ratings.rating) AS avgRating, COUNT(ratings.rating) AS countRating;

oneStarMovies = FILTER avgRatings BY avgRating < 2.0;

nameLookup = FOREACH metadata GENERATE movieID, movieTitle, ToUnixTime(ToDate(releaseDate, 'dd-MMM-yyyy'));

oneStarWithData = JOIN oneStarMovies BY movieID, nameLookup BY movieID;

mostPopularOneStartMoives = ORDER oneStarWithData BY oneStarMovies::countRating DESC;

DUMP mostPopularOneStartMoives;
