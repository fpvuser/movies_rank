package movies_rank

import scala.math.random

import org.apache.spark.sql.SparkSession

object MoviesRank {
  def main(args: Array[String]) {
    val spark = SparkSession
      .builder
      .appName("Movies rank")
      .getOrCreate()

    /* Parse json(with type: Array[Map[String -> String]]) to List of some key's values.
    	Example:
      jsonToList("key1")([{"key1": "first1", "key2": "second1"}, {"key1": "first2", "key2": "second2"}]) =
    	List("first1", "first2") */
    def jsonToList(key: String)(inputString: String): List[String] = {
      val map = new Gson().fromJson(inputString, classOf[java.util.List[java.util.Map[String, String]]])
      return map.toList.map(x => x(key).trim)
    }

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