package com.cloudera.examples
import java.io.FileInputStream
import java.util.Properties

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{current_timestamp, explode, split, window}
import scala.collection.JavaConversions.dictionaryAsScalaMap

object SparkKafkaSample {
  val properties = new Properties()

  def main(args: Array[String]) {
    if (args.length != 1) {
      System.err.println("Syntax: spark-submit <properties_file>")
      System.exit(1)
    }
    loadProperties(args(0))

    val spark = SparkSession
      .builder
      .appName("KafkaSparkExample")
      .getOrCreate()

    import spark.implicits._

    val df = spark
      .readStream
      .format("kafka")
      .options(kafkaProps("input"))
      .option("subscribe", inputTopic())
      .option("checkpointLocation", "/checkpoint/in")
      .load()

    // Write results to console
    val console = df
      .writeStream
      .outputMode("append")
      .format("console")
      .start()
    
    val ds = df
      .writeStream
      .format("csv")
      .option("path", "/tmp/fourthtopic")
      .option("checkpointLocation", "/checkpoint/out")
      .outputMode("append")
      .start()

    console.awaitTermination()
    ds.awaitTermination()
  }

  def loadProperties(path: String): Unit = {
    val is = new FileInputStream(path)
    properties.load(is)
  }

  def kafkaProps(prefix: String): Map[String, String] = {
    val regex = (prefix + raw"\.(.*)").r
    val partFunc: PartialFunction[(String, String), (String, String)] = {
      case (regex(k), v) => (k, v)
    }
    (for (p <- properties.map(p => (p._1.toString, p._2.toString)) if partFunc isDefinedAt p) yield partFunc(p)).toMap
  }

  def topics(key: String): String = {
    properties.getOrDefault(key, "").toString
  }

  def inputTopic(): String = {
    topics("inputTopic")
  }

  def outputTopic(): String = {
    topics("outputTopic")
  }
}