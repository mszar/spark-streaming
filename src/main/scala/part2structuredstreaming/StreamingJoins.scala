package part2structuredstreaming

import org.apache.spark.sql.SparkSession

object StreamingJoins {

  val spark = SparkSession.builder()
    .appName("Joins")
    .master("local[2]")
    .getOrCreate()


  val guitarPlayers = spark.read
    .option("inferSchema", true)
    .json("src/main/resources/data/guitarPlayers")

  val guitars = spark.read
    .option("inferSchema", true)
    .json("src/main/resources/data/guitars")

  val bands= spark.read
    .option("inferSchema", true)
    .json("src/main/resources/data/bands")

  val joinCondition = guitarPlayers.col("band ") === bands.col("id")
  val guitaristsBands = guitarPlayers.join(bands, joinCondition, "inner")

  def joinStreamWithStatic() = {
    val streamedBandsDF = spark.readStream
      .format("socket")
      .format()
  }


  def main(args: Array[String]): Unit = {

  }

}
