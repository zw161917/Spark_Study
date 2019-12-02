package kfk.spark.core

import org.apache.spark.{SparkConf, SparkContext}

object wwww {

  def main(args: Array[String]): Unit = {

  }

  def filter(): Unit ={

    val sparkConf = new SparkConf().setAppName("parallelize").setMaster("local");

    val sc = new SparkContext(sparkConf);

    val list = Array(1,2,3,4,5,6,7,8,9,10);

    val rdd = sc.parallelize(list);

    val filterValue = rdd.filter(x => (x % 2 == 0));

    filterValue.foreach(x => System.out.println(x));




  }
  def map(): Unit = {
    val sparkConf = new SparkConf().setAppName("parallelize").setMaster("local");

    val sc = new SparkContext(sparkConf);

    val list = Array(1,2,3,4);

    val rdd = sc.parallelize(list);

    val count = rdd.map(x => x * 2);

    count.foreach(x => System.out.println(x));



  }
}
