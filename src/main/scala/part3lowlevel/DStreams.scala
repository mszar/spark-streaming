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

  spark.sparkContext.setLogLevel("WARN")

  // Spark Streaming Context is a entry point to the DStreams API. Needs the sparkcontext and a duration (batch interval)
  val ssc = new StreamingContext(spark.sparkContext, Seconds(1))

  /*
  - define input sources by creating DStreams
  - define transformations on DStreams
  - call an action
  - start computation with ssc.start()
    - no more computations can be added
  - await termination, or stop computation
   */

  def readFromSocket() = {
    val socketStream: DStream[String] = ssc.socketTextStream("localhost", 12345)

    // transformation = lazy
    val wordsStream: DStream[String] = socketStream.flatMap(line => line.split(" "))

    // action
    // wordsStream.print()
    wordsStream.saveAsTextFiles("src/main/resources/data/words")

    ssc.start()
    ssc.awaitTermination()

  }

  def createNewFile() = {
    new Thread(() => {
      Thread.sleep(5000)

      val path = "src/main/resources/data/stocks"
      val dir = new File(path)
      var nFiles = dir.listFiles().length
      val newFile = new File(s"$path/newStocks$nFiles.csv")
      newFile.createNewFile()

      val writer = new FileWriter(newFile)
      writer.write(
        """
          |AMZN,Jun 1 2000,36.31
          |AMZN,Jul 1 2000,30.12
          |AMZN,Aug 1 2000,41.5
          |AMZN,Sep 1 2000,38.44
          |AMZN,Oct 1 2000,36.62
          |AMZN,Nov 1 2000,24.69
          |AMZN,Dec 1 2000,15.56
          |AMZN,Jan 1 2001,17.31
          |AMZN,Feb 1 2001,10.19
          |AMZN,Mar 1 2001,10.23
          |""".stripMargin.trim)
      writer.close()
    }).start()
  }


  def readFromFile() = {
    createNewFile() // operator on another thread
    val stocksFilePath = "src/main/resources/data/stocks"
    val textStream: DStream[String] = ssc.textFileStream(stocksFilePath)

    // transformations
    val dateFormat = new SimpleDateFormat("MMM d yyyy")

    val stocksStream: DStream[Stock] = textStream.map { line =>
      val tokens = line.split(",")
      val company = tokens(0)
      val date = new Date(dateFormat.parse(tokens(1)).getTime)
      val price = tokens(2).toDouble

      Stock(company, date, price)
    }

    // action
    stocksStream.print()

    // start the computation
    ssc.start()
    ssc.awaitTermination()



  }


  def main(args: Array[String]): Unit = {
    readFromSocket()
  }

}

