package org.poem

import org.apache.spark.sql.SparkSession

class Structured_Streaming {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("spark_structured_streaming").getOrCreate()


    val lines = spark.readStream.format("socket")
      .option("host", "localhost").option("port", "9999").load()

    import spark.implicits._
    val words = lines.as[String].flatMap(_.split(" "))
    val wordsCount = words.groupBy("values").count()

    val query = wordsCount.writeStream.outputMode("complete").format("console").start()
    query.awaitTermination()

  }
}
