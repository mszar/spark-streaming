package part2strucuredstreaming

import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Column, DataFrame, SparkSession}

object StreamingAggregations {


  val spark = SparkSession.builder()
    .appName("Streaming aggregations")
    .master("local[2]")
    .getOrCreate()
  spark.sparkContext.setLogLevel("WARN")

  def streamingCount() = {
    val lines: DataFrame = spark.readStream
      .format("socket")
      .option("host", "localhost")
      .option("port", 12345)
      .load()

    val lineCount: DataFrame = lines.selectExpr("count(*) as lineCount")

    lineCount.writeStream
      .format("console")
      .outputMode("complete")
      .start()
      .awaitTermination()

  }


  def numericalAggregations(aggFunction: Column => Column) = {
    val lines: DataFrame = spark.readStream
      .format("socket")
      .option("host", "localhost")
      .option("port", 12345)
      .load()

    val numbers = lines.select(col("value").cast("integer").as("number"))

    val aggregationDF = numbers.select(aggFunction(col("number")).as("agg_so_far"))

    aggregationDF.writeStream
      .format("console")
      .outputMode("complete")
      .start()
      .awaitTermination()

  }

  def groupNames() = {
    val lines: DataFrame = spark.readStream
      .format("socket")
      .option("host", "localhost")
      .option("port", 12345)
      .load()

    val names = lines.select(col("value").as("Name"))
      .groupBy(col("Name"))
      .count()

    names.writeStream
      .format("console")
      .outputMode("complete")
      .start()
      .awaitTermination()

  }


  def main(args: Array[String]): Unit = {
    groupNames()
  }

}
