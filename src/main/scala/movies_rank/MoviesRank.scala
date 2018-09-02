package movies_rank

import scala.math.random

import org.apache.spark.sql.SparkSession

object MoviesRank {
  def main(args: Array[String]) {
    val spark = SparkSession
      .builder
      .appName("Movies rank")
      .getOrCreate()

    val movies_df_raw = spark.read.format("csv")
	    .option("header", "true")
	    .option("multiLine", true)
	    .option("quote", "\"")
	    .option("escape", "\"")
	    .load("data/tmdb_5000_movies.csv")
	    .cache()

    spark.stop()
  }
}