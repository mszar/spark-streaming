package part3lowlevel

import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.{Seconds, StreamingContext}

object DStreams {

  val spark = SparkSession.builder()
    .appName("DStreams")
    .master("local[2]")
    .getOrCreate()

  /*

  Spark Streaming Context = entry point to the DStreams API
  - needs the spark context
  - a duration = batch interval
  */

  val ssc = new StreamingContext(spark.sparkContext, Seconds(1))

  /*
    - define input sources by creating DStreams
    - define transformations on DStreams
    - start ALL computations with ssc.start()
      - no more computations can be added
    - await termination or stop the computation
      - you cannon restart the ssc
   */



  def main(args: Array[String]): Unit = {


  }

}
