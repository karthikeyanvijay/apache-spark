package com.cloudera.spark.examples
import org.apache.spark.sql.SparkSession

/**
 * @author Vijay Anand Karthikeyan
 */
object file2hive {
  
  def foo(x : Array[String]) = x.foldLeft("")((a,b) => a + b)
  final val sparkApp = "file2hive"

  def main(args : Array[String]) {
    //println("concat arguments = " + foo(args))
    val spark = SparkSession.builder
      .master("local[*]")
      .appName(sparkApp)
      .config("spark.sql.warehouse.dir", "/tmp/")
      .config("javax.jdo.option.ConnectionURL", "jdbc:mysql://nightly71x-1.nightly71x.root.hwx.site:3306/hive1")
      .config("javax.jdo.option.ConnectionUserName", "hive1")
      .config("javax.jdo.option.ConnectionPassword", "hive1")
      .config("javax.jdo.option.ConnectionDriverName", "com.mysql.jdbc.Driver")
      .config("spark.driver.bindAddress", "127.0.0.1")
      //.config("spark.hive.exec.scratchdir", "/tmp/hive")
      .enableHiveSupport()
      .getOrCreate()
     spark.sql("show databases").show()

  }

}
