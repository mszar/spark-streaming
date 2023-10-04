package part2structuredstreaming

import org.apache.spark.sql.{DataFrame, Dataset, Encoders, SparkSession}
import org.apache.spark.sql.functions._
import common._

object StreamingDatasets {

  val spark = SparkSession.builder()
    .appName("Streaming Datasets")
    .master("local[2]")
    .getOrCreate()

  import spark.implicits._
  def readCars(): Dataset[Car] = {
    val carEncoder = Encoders.product[Car]
    spark.readStream
      .format("socket")
      .option("host", "localhost")
      .option("port", 12345)
      .load()
      .select(from_json(col("value"), carsSchema).as("car"))
      .selectExpr("car.*")
      // .as[Car](carEncoder)
      .as[Car]
  }

  def showCarNames() = {

    val carsDS: Dataset[Car] = readCars()

    val carNamesDF: DataFrame = carsDS.select(col("Name"))
    val carNamesAlt: Dataset[String] = carsDS.map(_.Name)

    carNamesAlt.writeStream
      .format("console")
      .outputMode("append")
      .start()
      .awaitTermination()
  }

  def powerfulCars() = {

    val carsDS: Dataset[Car] = readCars()
    carsDS.filter(_.Horsepower.getOrElse(0L) > 140)
      .writeStream
      .format("console")
      .outputMode("append")
      .start()
      .awaitTermination()

  }

  def avgHP() = {
    val carsDS = readCars()

    carsDS.select(avg(col("Horsepower")))
      .writeStream
      .format("console")
      .outputMode("complete")
      .start()
      .awaitTermination()


  }

  def countOrigin() = {
    val carsDS = readCars()

    val carCountByOrigin = carsDS.groupBy(col("Origin")).count() // option 1
    val carCountByOriginAlt = carsDS.groupByKey(car => car.Origin).count()

    carCountByOriginAlt
      .writeStream
      .format("console")
      .outputMode("complete")
      .start()
      .awaitTermination()



  }


  def main(args: Array[String]): Unit = {
    countOrigin()
  }

}
