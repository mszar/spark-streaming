package part3lowlevel

import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.{Seconds, StreamingContext}

object DStreamsWindowTransformation {

  val spark = SparkSession.builder()
    .appName("Window Functions")
    .master("local[2]")
    .getOrCreate()

  val ssc = new StreamingContext(spark.sparkContext, Seconds(1))

  def readLines() = ssc.socketTextStream("localhost", 12345)

  def linesByWindow() = readLines().window(Seconds(10))

  def linesBySlidingWindow() = readLines().window(Seconds(10), Seconds(5))


  def linesByWindowCount() = readLines().window(Seconds(10)).count()

  def main(args: Array[String]): Unit = {
    linesByWindowCount.print()
    ssc.start()
    ssc.awaitTermination()
  }

}
