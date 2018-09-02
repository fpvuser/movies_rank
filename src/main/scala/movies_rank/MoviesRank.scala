package movies_rank

import scala.math.random

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._ 
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.expressions.Window
import com.google.gson.Gson
import scala.collection.JavaConversions._
import scala.math.sqrt

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

	val avgVoteCount: Double = movies_df.agg(avg(col("vote_count"))).collect().head(0).asInstanceOf[Double]

    spark.stop()
  }
}