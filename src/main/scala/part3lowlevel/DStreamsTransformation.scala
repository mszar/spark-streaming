package part3lowlevel

import common._
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.{Seconds, StreamingContext}

import java.sql.Date

object DStreamsTransformation {

  val spark = SparkSession.builder()
    .appName("DStreams")
    .master("local[2]")
    .getOrCreate()

  val ssc = new StreamingContext(spark.sparkContext, Seconds(1))

  def readPeople() = ssc.socketTextStream("localhost", 12345).map { line =>
    val tokens = line.split(":")
    Person(
      tokens(0).toInt,
      tokens(1),
      tokens(2),
      tokens(3),
      tokens(4),
      Date.valueOf(tokens(5)),
      tokens(6),
      tokens(7).toInt
    )
  }


  def main(args: Array[String]): Unit = {
    val stream = readPeople()
    stream.print()
    ssc.start()
    ssc.awaitTermination()
  }

}
