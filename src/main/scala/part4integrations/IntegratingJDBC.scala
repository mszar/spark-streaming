package part4integrations

import common.{Car, carsSchema}
import org.apache.spark.sql.{Dataset, SparkSession}

object IntegratingJDBC {

  val spark = SparkSession.builder()
    .appName("JDBC")
    .master("local[2]")
    .getOrCreate()

  import spark.implicits._

  val driver = "org.postgresql.Driver"
  val url = "jdbc:postgresql://localhost:5432/rtjvm"
  val user = "docker"
  val password = "docker"

  def writeStreamToPostgres() = {
    val carsDF = spark.readStream
      .schema(carsSchema)
      .json("src/main/resources/data/cars")

    val carsDS = carsDF.as[Car]

    carsDS.writeStream
      .foreachBatch{ (batch: Dataset[Car], _: Long) =>
        batch.write
          .format("jdbc")
          .option("driver", driver)
          .option("url", url)
          .option("user", user)
          .option("password", password)
          .option("dbtable", "public.cars")
          .save()
      }
      .start()
      .awaitTermination()


  }

  def main(args: Array[String]): Unit = {
    writeStreamToPostgres()
  }

}
