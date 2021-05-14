package com.cloudera.spark.examples
import org.apache.spark.sql.SparkSession

/**
 * @author Vijay Anand Karthikeyan
 *         Dataset -> https://www.kaggle.com/dhruvildave/wikibooks-dataset
 */
object loadfile2hive {

  final val sparkApp = "loadfile2hive"

  def main(args : Array[String]) {

    val file_path = args(0)
    val file_type = args(1)
    val table_name = args(2)
    val output_format = args(3)
    val spark = SparkSession.builder
      //.master("local[*]") .config("spark.driver.bindAddress", "127.0.0.1")
      .appName(sparkApp)
      .enableHiveSupport()
      .getOrCreate()

    val data = spark.read
      .format(file_type)
      .option("header", "true")
      .option("inferSchema", "true")
      .load(file_path)

    data.write
      .format(output_format)
      .mode("overwrite")
      .saveAsTable(table_name)

    spark.sql("select count(1) from " + table_name).show();
  }

}
