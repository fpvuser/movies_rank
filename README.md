# movies_rank

## Run project


Create directory "movies_rank/data/" and save file tmdb_5000_movies.csv there. (get file here: https://www.kaggle.com/tmdb/tmdb-movie-metadata)

then in top project directory run:

`sbt compile package`

`spark-submit --class movies_rank.MoviesRank --master local[*] ./target/scala-2.11/movies_rank_2.11-0.0.1.jar`
