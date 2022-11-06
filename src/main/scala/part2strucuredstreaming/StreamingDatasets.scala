package part2strucuredstreaming
import org.apache.spark.sql.{DataFrame, Dataset, Encoders, SparkSession}
import org.apache.spark.sql.functions._
import common._

object StreamingDatasets {

  val spark = SparkSession.builder()
    .appName("Streaming Datasets")
    .master("local[2]")
    .getOrCreate()

  spark.sparkContext.setLogLevel("WARN")

  // include encoders for DF -> DS transformations
  import spark.implicits._

  def readCars(): Dataset[Car] = {
    // useful for DF -> DS transformations
    val carEncoder = Encoders.product[Car]

    spark.readStream
      .format("socket")
      .option("host", "localhost")
      .option("port", 12345)
      .load() // DF with single string column "value"
      .select(from_json(col("value"), carsSchema).as("car")) // composite column (struct)
      .selectExpr("car.*") // DF with multiple columns
      .as[Car] // encoder can be passed implicitly with spark.implicits
  }

  def showCarNames() = {
    val carsDS: Dataset[Car] = readCars()

    // transformations here
    val carNamesDF: DataFrame = carsDS.select(col("Name")) // DF

    // collection transformations maintain type info
    val carNamesAlt: Dataset[String] = carsDS.map(_.Name)

    carNamesAlt.writeStream
      .format("console")
      .outputMode("append")
      .start()
      .awaitTermination()
  }


  def main(args: Array[String]): Unit = {
    showCarNames()
  }

}
