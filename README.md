# movies_rank

## Run project

In top project directory run:

`sbt package`

`spark-submit --class movies_rank.MoviesRank --master local[*] ./target/scala-2.11/movies_rank_2.11-0.0.1.jar`
