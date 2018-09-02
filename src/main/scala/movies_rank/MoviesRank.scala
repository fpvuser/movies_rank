package movies_rank

import scala.math.random

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._ 
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.expressions.{Window, UserDefinedFunction}
import com.google.gson.Gson
import scala.collection.JavaConversions._
import scala.math.sqrt

object MoviesRank extends SparkSessionWrapper{
	import spark.implicits._

	def main(args: Array[String]) {
		val moviesRawDf = readRawMoviesDf("data/tmdb_5000_movies.csv")
		val moviesDf = processMoviesDf(moviesRawDf)

		  /* x/(x+alpha) is sigmoid function.
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

		val companiesScore = calculateCompaniesScoreDf(moviesDf, 
			voteCountFactor, 
			moviesCountFactor)

		val companiesScoreArray = companiesScore.collect()
		for(m <- companiesScoreArray) yield println(m(0) + "\t" + m(1))

		spark.stop()
	}
		/*  Using a sigmoid to underestimate the evaluation of companies 
		    with a small number of votes or a small number of movies.

		    voteAvgSeeingCount = vote_average * sigmoid(alpha=29, x=vote_count)
		    score = avg(voteAvgSeeingCount) * sigmoid(alpha=1, x=movies_count) */
	def calculateCompaniesScoreDf(moviesDf: DataFrame, 
			voteCountFactor: UserDefinedFunction,
			moviesCountFactor: UserDefinedFunction): DataFrame = {
		moviesDf
			.withColumn("voteAvgSeeingCount", $"vote_average" * voteCountFactor($"vote_count"))
			.groupBy("companies")
			.agg(avg("voteAvgSeeingCount").as("processed_vote_average"),
			 	 max("movies_count").as("movies_count"))
			.withColumn("score", $"processed_vote_average" * moviesCountFactor($"movies_count"))
			.select("companies", "score")
			.orderBy(desc("score"))
	}

		// Assign types to columns and extract names from "production_companies" column.
	def processMoviesDf(rawDf: DataFrame): DataFrame = {

			// json schema in production_companies column.
		val productionCompaniesSchema = ArrayType(
			StructType(Seq(
				$"name".string,
				$"id".int)))
			// Parse json to Array of Maps and explode it.
		val companyCol = explode(from_json($"production_companies", productionCompaniesSchema))

		rawDf.where($"id".isNotNull)
			.withColumn("company", companyCol)
			.select($"id".cast(IntegerType),
					$"company.name".as("companies"),
					$"vote_average".cast(FloatType),
					$"vote_count".cast(IntegerType))
			.withColumn("movies_count", count("id") over Window.partitionBy("companies"))
			.cache()
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