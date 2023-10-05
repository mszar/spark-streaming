package part3lowlevel

import common._
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Seconds, StreamingContext}

import java.io.File
import java.sql.Date
import java.time.{LocalDate, Period}

object DStreamsTransformation {

  val spark = SparkSession.builder()
    .appName("DStreams")
    .master("local[2]")
    .getOrCreate()

  val ssc = new StreamingContext(spark.sparkContext, Seconds(1))

import spark.implicits._
  def readPeople(): DStream[Person] = ssc.socketTextStream("localhost", 12345).map { line =>
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

  // map, flatMap, filter
  def peopleAges(): DStream[(String, Int)] = readPeople().map { person =>
    val age = Period.between(person.birthDate.toLocalDate, LocalDate.now()).getYears
    (s"${person.firstName} ${person.lastName}", age)
  }

  def peopleSmallNames(): DStream[String] = readPeople().flatMap { person =>
    List(person.firstName, person.middleName)
  }

  def highIncomePeople() = readPeople().filter(_.salary > 80000)

  def countPeople() = readPeople().count()

  def countNames(): DStream[(String, Long)] = readPeople().map(_.firstName).countByValue()

  def countNamesReduce(): DStream[(String, Int)] =
    readPeople()
      .map(_.firstName)
      .map(name => (name, 1))
      .reduceByKey((a, b) => a + b)

  def saveToJson() = readPeople()
    .foreachRDD { rdd =>
      val ds = spark.createDataset(rdd)
      val f = new File("src/main/resources/data/people")
      val nFiles = f.listFiles().length
      val path = s"src/main/resources/data/people/people$nFiles.json"

      ds.write.json(path)
    }


  def main(args: Array[String]): Unit = {
//    val stream = saveToJson()
//    stream.print()
    saveToJson()

    ssc.start()
    ssc.awaitTermination()
  }

}
