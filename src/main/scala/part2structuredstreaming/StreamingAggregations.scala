package part2structuredstreaming

import org.apache.spark.sql.{Column, SparkSession}
import org.apache.spark.sql.functions._

object StreamingAggregations {

  val spark = SparkSession.builder()
    .appName("Agg")
    .master("local[2]")
    .getOrCreate()

  def streamingCount() = {
    val lines = spark.readStream
      .format("socket")
      .option("host", "localhost")
      .option("port", 12345)
      .load()

    val lineCount = lines.selectExpr("count(*) as  lineCount")

    lineCount.writeStream
      .format("console")
      .outputMode("complete")
      .start()
      .awaitTermination()

  }


  def numericalAggregations(aggFunction: Column => Column) = {
    val lines = spark.readStream
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

  def groupName() = {
    val lines = spark.readStream
      .format("socket")
      .option("host", "localhost")
      .option("port", 12345)
      .load()

    val names = lines
      .select(col("value").as("name"))
      .groupBy(col("name"))
      .count()

    names.writeStream
      .format("console")
      .outputMode("complete")
      .start()
      .awaitTermination()


  }



  def main(args: Array[String]): Unit = {
    groupName()
  }

}
