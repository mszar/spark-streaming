package part3lowlevel

import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Seconds, StreamingContext}

object DStreams {

  val spark = SparkSession.builder()
    .appName("DStreams")
    .master("local[2]")
    .getOrCreate()

  /*

  Spark Streaming Context = entry point to the DStreams API
  - needs the spark context
  - a duration = batch interval
  */

  val ssc = new StreamingContext(spark.sparkContext, Seconds(1))

  /*
    - define input sources by creating DStreams
    - define transformations on DStreams
    - call an action
    - start ALL computations with ssc.start()
      - no more computations can be added
    - await termination or stop the computation
      - you cannon restart the ssc
   */

  def readFromSocket() = {
    val socketStream: DStream[String] = ssc.socketTextStream("localhost", 12345)

    // transformation = lazy
    val wordsStream: DStream[String] = socketStream.flatMap(line => line.split(" "))

    // action
    wordsStream.print()

    ssc.start()
    ssc.awaitTermination()

  }

  def readFromFile() = {
    val stocksFilePath = "src/main/resources/data/stocks"
    val textStream: DStream[String] = ssc.textFileStream(stocksFilePath)

    // transformation
    // val dateFormat
    val stockStream = textStream.map { line =>
      val tokens = line.split(",")
      val company = tokens(0)
      val date =

    }


  }


  def main(args: Array[String]): Unit = {
    readFromSocket()

  }

}
