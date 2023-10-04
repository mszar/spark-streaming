package part2structuredstreaming

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

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

  val joinCondition = guitarPlayers.col("band") === bands.col("id")
  val guitaristsBands = guitarPlayers.join(bands, joinCondition, "inner")
  val bandsSchema = bands.schema
  val guitarPlayersSchema = guitarPlayers.schema

  def joinStreamWithStatic() = {
    val streamedBandsDF = spark.readStream
      .format("socket")
      .option("host", "localhost")
      .option("port", 12345)
      .load()
      .select(from_json(col("value"), bandsSchema).as("band"))
      .selectExpr("band.id as id", "band.name as name", "band.hometown as hometown", "band.year as year")

    val streamJoin = guitarPlayers.col("band") === streamedBandsDF.col("id")
    val streamedBandsGuitaristsDF = streamedBandsDF.join(guitarPlayers, streamJoin, "inner")

    streamedBandsGuitaristsDF.writeStream
      .format("console")
      .outputMode("append")
      .start()
      .awaitTermination()

  }

  def joinStreamWithStream() = {

    val streamedBandsDF = spark.readStream
      .format("socket")
      .option("host", "localhost")
      .option("port", 12345)
      .load()
      .select(from_json(col("value"), bandsSchema).as("band"))
      .selectExpr("band.id as id", "band.name as name", "band.hometown as hometown", "band.year as year")

    val streamedGuitaristsDF = spark.readStream
      .format("socket")
      .option("host", "localhost")
      .option("port", 12346)
      .load()
      .select(from_json(col("value"), guitarPlayersSchema).as("guitarPlayer"))
      .selectExpr("guitarPlayer.id as id", "guitarPlayer.name as name", "guitarPlayer.guitars as guitars", "guitarPlayer.band as band")

    val svssJoin = streamedGuitaristsDF.col("band") === streamedBandsDF.col("id")
    val streamedJoin = streamedBandsDF.join(streamedGuitaristsDF, svssJoin)

    streamedJoin.writeStream
      .format("console")
      .outputMode("append")
      .start()
      .awaitTermination()

  }

  def main(args: Array[String]): Unit = {
    joinStreamWithStream()
  }

}
