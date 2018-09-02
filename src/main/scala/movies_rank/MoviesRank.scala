package movies_rank

import scala.math.random

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._ 
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.expressions.Window
import com.google.gson.Gson
import scala.collection.JavaConversions._
import scala.math.sqrt

object MoviesRank {
	def main(args: Array[String]) {
		implicit val spark = SparkSession
		  .builder
		  .appName("Movies rank")
		  .getOrCreate()

		import spark.implicits._

		/* Parse json(with type: Array[Map[String -> String]]) to List of some key's values.
			Example:
		  jsonToList("key1")([{"key1": "first1", "key2": "second1"}, {"key1": "first2", "key2": "second2"}]) =
			List("first1", "first2") */
		val productionCompaniesSchema = ArrayType(
		    StructType(Seq(
		      $"name".string,
		      $"id".int)))

		// This UDF takes only names from json.
		val companyCol = explode(from_json($"production_companies", productionCompaniesSchema)) //("name").as("companies")

		val moviesRawDf = readRawMoviesDf("data/tmdb_5000_movies.csv")

		// Assign types to columns and extract names from "production_companies" column.
		val moviesDf = moviesRawDf.where($"id".isNotNull)
			.withColumn("company", companyCol)
			.select($"id".cast(IntegerType),
			$"company.name".as("companies"),
			$"vote_average".cast(FloatType),
			$"vote_count".cast(IntegerType))
			.withColumn("movies_count", count("id") over Window.partitionBy("companies")).cache()

		  /* x/(x+alpha) is sigmoid function.
		  	 Using a sigmoid to underestimate the evaluation of companies 
		     with a small number of votes or a small number of movies.
		       alpha - sigmoid parameter. Bigger alpha - flatter sigmoid.
		     Examples:
		       sigmoid(alpha = 1, x = 5) = 0.8333...
		       sigmoid(alpha = 1, x = 10) = 0.909...
		       sigmoid(alpha = 1, x = 50) = 0.9803...
		       sigmoid(alpha = 29, x = 5) = 0.147...
		       sigmoid(alpha = 29, x = 10) = 0.256...
		       sigmoid(alpha = 29, x = 50) = 0.632... */
		val voteCountFactor = udf[Double, Double](x => x / (x + 29))
		val moviesCountFactor = udf[Double, Double](x => x / (x + 1))

		val companiesScore = moviesDf
		.withColumn("voteAvgSeeingCount", $"vote_average" * voteCountFactor($"vote_count"))
			.groupBy("companies")
			.agg(avg("voteAvgSeeingCount").as("processed_vote_average"),
			 	 max("movies_count").as("movies_count"))
			.withColumn("score", $"processed_vote_average" * moviesCountFactor($"movies_count"))
			.select("companies", "score")
			.orderBy(desc("score"))

		val companiesScoreArray = companiesScore.collect()
		for(m <- companiesScoreArray) yield println(m(0) + "\t" + m(1))

		spark.stop()
	}

  	def readRawMoviesDf(inputFile: String)(implicit spark: SparkSession): DataFrame = {
		spark.read.format("csv")
		.option("header", "true")
		.option("multiLine", true)
		.option("quote", "\"")
		.option("escape", "\"")
		.option("inferSchema", "true").load(inputFile)
		.cache()
	}

  //def readFromCSV(fileName: String): DataFrame
}