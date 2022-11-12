package part3lowlevel

import common.Person
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.dstream.DStream

import java.sql.Date
import org.apache.spark.streaming.{Seconds, StreamingContext}

import java.time.{LocalDate, Period}

object DStreamsTransformations {


  val spark = SparkSession.builder()
    .appName("DStreams Transformations")
    .master("local[2]")
    .getOrCreate()

  spark.sparkContext.setLogLevel("WARN")

  val ssc = new StreamingContext(spark.sparkContext, Seconds(1))


  def readPeople(): DStream[Person] = ssc.socketTextStream("localhost", 9999).map { line =>
    val tokens = line.split(":")
    Person(
      tokens(0).toInt, // id
      tokens(1), // first name
      tokens(2), // middle name
      tokens(3), // last name
      tokens(4), // gender
      Date.valueOf(tokens(5)), // birth
      tokens(6), // uuid
      tokens(7).toInt // salary
    )
  }

  // map, flatMap, filter

  def peopleAges(): DStream[(String, Int)] = readPeople().map { person =>
    val age = Period.between(person.birthDate.toLocalDate, LocalDate.now()).getYears
    (s"${person.firstName} ${person.lastName}", age)
  }


  def peopleSmallNames(): DStream[String] = readPeople().flatMap { person =>
    List(person.firstName, person.middleName)
  }

  def main(args: Array[String]): Unit = {
    val stream = peopleSmallNames()
    stream.print()
    ssc.start()
    ssc.awaitTermination()
  }

}
