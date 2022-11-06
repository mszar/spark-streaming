package part2strucuredstreaming

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._


object StreamingJoins {

  val spark = SparkSession.builder()
    .appName("Streaming Joins")
    .master("local[2]")
    .getOrCreate()

  spark.sparkContext.setLogLevel("WARN")

  val guitarPlayers = spark.read
    .option("inferSchema", true)
    .json("src/main/resources/data/guitarPlayers")

  val guitars = spark.read
    .option("inferSchema", true)
    .json("src/main/resources/data/guitars")

  val bands = spark.read
    .option("inferSchema", true)
    .json("src/main/resources/data/bands")

  // joining static DFs
  val joinCondition = guitarPlayers.col("band") === bands.col("id")
  val guitaristsBands = guitarPlayers.join(bands, joinCondition, "inner")

  val bandsSchema = bands.schema

  def joinStreamWithStatic() = {

    val streamedBandDF = spark.readStream
      .format("socket")
      .option("host", "localhost")
      .option("port", 12345)
      .load()
      .select(from_json(col("value"), bandsSchema).as("band"))
      .selectExpr("band.id as id", "band.name as name", "band.hometown as hometown", "band.year as year")

    val streamedBandsGuitaristsDF = streamedBandDF.join(guitarPlayers, guitarPlayers.col("band") === streamedBandDF.col("id"), "inner")

    streamedBandsGuitaristsDF.writeStream
      .format("console")
      .outputMode("append")
      .start()
      .awaitTermination()

  }

  def joinSteamWithStream() = {
    val streamedBandDF = spark.readStream
      .format("socket")
      .option("host", "localhost")
      .option("port", 12345)
      .load()
      .select(from_json(col("value"), bandsSchema).as("band"))
      .selectExpr("band.id as id", "band.name as name", "band.hometown as hometown", "band.year as year")

    val streamedGuitaristDF = spark.readStream
      .format("socket")
      .option("host", "localhost")
      .option("port", 12346)
      .load()
      .select(from_json(col("value"), guitarPlayers.schema).as("guitarPlayers"))
      .selectExpr("guitarPlayers.id as id", "guitarPlayers.name as name", "guitarPlayers.guitars as guitars", "guitarPlayers.band as band")

    val streamedJoin = streamedBandDF.join(streamedGuitaristDF, streamedGuitaristDF.col("band") === streamedBandDF.col("id"))

    streamedJoin.writeStream
      .format("console")
      .outputMode("append")
      .start()
      .awaitTermination()


  }



  def main(args: Array[String]): Unit = {
    joinSteamWithStream()
  }
}
