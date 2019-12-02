package kfk.spark.core

import kfk.spark.common.CommSparkContextSca

object DistinctScala {
  def main(args: Array[String]): Unit = {
    val sc = CommSparkContextSca.getsc();
    val list1 = Array("cherry", "herry", "leo","ben", "cherry");

    val rdd1 = for (elem <- sc.parallelize(list1).distinct().collect()) {
      System.out.println(elem)
    }
  }
}
