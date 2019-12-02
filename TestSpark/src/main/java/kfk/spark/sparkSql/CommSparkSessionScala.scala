package kfk.spark.sparkSql

import org.apache.spark.sql.SparkSession

object CommSparkSessionScala {

  def main(args: Array[String]): Unit = {

  }
  def getSparkSession(): SparkSession = {
    val spark = SparkSession.builder()
      .appName("test")
      .master("local")
      .config("spark.sql.warehouse.dir", "/Users/caojinbo/Documents/spark/spark-warehouse")
      .getOrCreate();
       spark
  }
}
