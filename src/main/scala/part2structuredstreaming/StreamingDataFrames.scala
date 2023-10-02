package part2structuredstreaming

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import common._
import org.apache.spark.sql.streaming.Trigger
import scala.concurrent.duration._

object StreamingDataFrames {

  val spark = SparkSession.builder()
    .appName("First stream")
    .master("local[2]")
    .getOrCreate()

//   spark.sparkContext.setLogLevel("WARN")


  def readFromSocket() = {
    val lines = spark.readStream
      .format("socket")
      .option("host", "localhost")
      .option("port", 12345)
      .load()

    val shortLines = lines.filter(length(col("value")) <= 5)

    println(shortLines.isStreaming)

    val query = shortLines.writeStream
      .format("console")
      .outputMode("append")
      .start()

    query.awaitTermination()

  }

  def readFromFiles() = {
    val stockDF = spark.readStream
      .format("csv")
      .option("header", "false")
      .option("dateFormat", "MMM d yyyy")
      .schema(stocksSchema)
      .load("src/main/resources/data/stocks")

    stockDF.writeStream
      .format("console")
      .outputMode("append")
      .start()
      .awaitTermination()

  }

def demoTriggers() = {
  val lines = spark.readStream
    .format("socket")
    .option("host", "localhost")
    .option("port", 12345)
    .load()

  lines.writeStream
    .format("console")
    .outputMode("append")
    .trigger(
//      Trigger.ProcessingTime(2.seconds)
//      Trigger.Once()
    Trigger.Continuous(2.seconds)
    )
    .start()
    .awaitTermination()
}


  def main(args: Array[String]): Unit = {
    demoTriggers()
  }

}
