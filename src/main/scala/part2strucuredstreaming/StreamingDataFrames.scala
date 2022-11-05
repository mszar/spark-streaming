package part2strucuredstreaming

import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}
import common._
import org.apache.spark.sql.streaming.Trigger

import scala.concurrent.duration._

object StreamingDataFrames {

  val spark = SparkSession.builder()
    .appName("Our first stream")
    .master("local[2]")
    .getOrCreate()

  def readFromSocket() = {
    // reading DF
    val lines: DataFrame = spark.readStream
      .format("socket")
      .option("host", "localhost")
      .option("port", 12345)
      .load()

    //transformation
    val shortLines = lines.filter(length(col("value")) <= 5)

    // tel between a static vs streaming DF
    println(shortLines.isStreaming)

    // consuming a DF
    val query = shortLines.writeStream
      .format("console")
      .outputMode("append")
      .start()

    // wait for the stream to finish
    query.awaitTermination()
  }

  def readFromFiles()= {
    val stocksDF: DataFrame = spark.readStream
      .format("csv")
      .option("header", "false")
      .option("dateFormat", "MMM d yyyy")
      .schema(stocksSchema)
      .load("src/main/resources/data/stocks")

    stocksDF.writeStream
      .format("console")
      .outputMode("append")
      .start()
      .awaitTermination()
  }


  def demoTriggers() = {
    val lines: DataFrame = spark.readStream
      .format("socket")
      .option("host", "localhost")
      .option("port", 12345)
      .load()

    lines.writeStream
      .format("console")
      .outputMode("append")
      .trigger(
        // Trigger.ProcessingTime(2.seconds))
        // Trigger.Once()
        Trigger.Continuous(2.seconds))
      .start()
      .awaitTermination()

  }



  def main(args: Array[String]): Unit = {
    readFromFiles()

  }

}
