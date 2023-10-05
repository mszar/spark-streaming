package part3lowlevel

import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Seconds, StreamingContext}

import java.sql.Date
import java.text.SimpleDateFormat
import common._

import java.io.{File, FileWriter}

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
    // wordsStream.print()

    wordsStream.saveAsTextFiles("src/main/resources/data/words/") // each folder = rdd = batch, each file = partition


    ssc.start()
    ssc.awaitTermination()

  }

  def createNewFile() = {
    new Thread( () => {
      Thread.sleep(5000)
      val path = "src/main/resources/data/stocks"
      val dir = new File(path)
      val nFiles = dir.listFiles().length
      val newFile = new File(s"$path/newStocks$nFiles.csv")
      newFile.createNewFile()

      val writer = new FileWriter(newFile)
      writer.write(
        """
          |AAPL,Jan 1 2001,10.81
          |AAPL,Feb 1 2001,9.12
          |AAPL,Mar 1 2001,11.03
          |AAPL,Apr 1 2001,12.74
          |AAPL,May 1 2001,9.98
          |AAPL,Jun 1 2001,11.62
          |AAPL,Jul 1 2001,9.4
          |AAPL,Aug 1 2001,9.27
          |""".stripMargin.trim)

      writer.close()

    }).start()
  }

  def readFromFile() = {

    createNewFile()

    val stocksFilePath = "src/main/resources/data/stocks"
    val textStream: DStream[String] = ssc.textFileStream(stocksFilePath)

    // transformation
    val dateFormat = new SimpleDateFormat("MMM d yyyy")
    val stockStream: DStream[Stock] = textStream.map { line =>
      val tokens = line.split(",")
      val company = tokens(0)
      val date = new Date(dateFormat.parse(tokens(1)).getTime)
      val price = tokens(2).toDouble

      Stock(company, date, price)

    }

    stockStream.print()
    ssc.start()
    ssc.awaitTermination()equals()

  }


  def main(args: Array[String]): Unit = {
    readFromSocket()

  }

}
