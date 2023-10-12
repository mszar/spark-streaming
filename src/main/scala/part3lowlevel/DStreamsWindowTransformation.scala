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

  def sumAllTextByWindow() = readLines()
    .map(_.length)
    .window(Seconds(10), Seconds(5))
    .reduce(_ + _)

  def sumAllTextByWindowAlt() = readLines()
    .map(_.length)
    .reduceByWindow(_ + _, Seconds(10), Seconds(5))

  def linesByTumblingWindow() = readLines()
    .window(Seconds(10), Seconds(10))

  def computeWordOcurrences() = {

    // for reducebykeyandwindow i need checkpoint directory set
    ssc.checkpoint("checkpoints")

    readLines()
      .flatMap(_.split(" "))
      .map(word => (word, 1))
      .reduceByKeyAndWindow(
        (a, b) => a + b, // reduction function
        (a, b) => a - b, // "inverse" function
        Seconds(60), // window duration
        Seconds(30) // sliding duration
      )
  }

  val moneyPerExpensiveWord = 2

  def showMeTheMoney() = readLines()
    .flatMap(line => line.split(" "))
      .filter(_.length >= 10)
      .map(_ => moneyPerExpensiveWord)
      .reduce(_ + _)
      .window(Seconds(30), Seconds(10))
      .reduce(_ + _)

  def showMeTheMoney2() = readLines()
    .flatMap(line => line.split(" "))
    .filter(_.length >= 10)
    .countByWindow(Seconds(30), Seconds(10))
    .map(_ * moneyPerExpensiveWord)

  def showMeTheMoney3() = readLines()
    .flatMap(line => line.split(" "))
    .filter(_.length >= 10)
    .map(_ => moneyPerExpensiveWord)
    .reduceByWindow(_ + _, Seconds(30), Seconds(10))

  def showMeTheMoney4() = {

    ssc.checkpoint("checkpoints")

    readLines()
      .flatMap(line => line.split(" "))
      .filter(_.length >= 10)
      .map { word =>
        if (word.length >= 10) ("expensive", 2)
        else ("cheap", 1)
      }
      .reduceByKeyAndWindow(_ + _,_ - _, Seconds(30), Seconds(10))
  }



  def main(args: Array[String]): Unit = {
    computeWordOcurrences.print()
    ssc.start()
    ssc.awaitTermination()
  }

}
