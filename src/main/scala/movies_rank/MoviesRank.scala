package movies_rank

import scala.math.random

import org.apache.spark.sql.SparkSession

object MoviesRank {
  def main(args: Array[String]) {
    val spark = SparkSession
      .builder
      .appName("Movies rank")
      .getOrCreate()

    spark.stop()
  }
}