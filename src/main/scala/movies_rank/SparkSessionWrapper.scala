package movies_rank

import org.apache.spark.sql.SparkSession

trait SparkSessionWrapper {

	implicit lazy val spark: SparkSession = {
		SparkSession
			.builder()
			.appName("Movies rank")
			.getOrCreate()
	}

}