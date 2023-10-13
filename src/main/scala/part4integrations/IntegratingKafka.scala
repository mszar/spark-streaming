package part4integrations

import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.{Seconds, StreamingContext}

object IntegratingKafka {

  val spark: SparkSession = SparkSession.builder()
    .appName("Kafka Integration")
    .master("local[2]")
    .getOrCreate()

  def readFromKafka(): Unit = {

    val kafkaDF = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe", "rockthejvm")
      .load()

    kafkaDF.writeStream
      .format("console")
      .outputMode("append")
      .start()
      .awaitTermination()

  }

  def main(args: Array[String]): Unit = {
    readFromKafka()
  }

}
