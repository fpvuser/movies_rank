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

      // This UDF takes only names from json.
    val jsonToListOfNamesUDF = udf[List[String], String](jsonToList("name"))

    val movies_df_raw = spark.read.format("csv")
	    .option("header", "true")
	    .option("multiLine", true)
	    .option("quote", "\"")
	    .option("escape", "\"")
	    .load("data/tmdb_5000_movies.csv")
	    .cache()

      // Assign types to columns and extract names from "production_companies" column.
    val movies_df = movies_df_raw.where(col("id").isNotNull)
	    .select(col("id").cast(IntegerType), 
	            explode(jsonToListOfNamesUDF(col("production_companies"))).as("companies"), 
	            col("vote_average").cast(FloatType), 
	            col("vote_count").cast(IntegerType))
	    .withColumn("movies_count", count("id") over Window.partitionBy("companies"))
	    .cache()

    spark.stop()
  }
}