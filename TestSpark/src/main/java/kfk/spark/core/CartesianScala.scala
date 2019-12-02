package kfk.spark.core

import kfk.spark.common.CommSparkContextSca

object CartesianScala {
  def main(args: Array[String]): Unit = {
    val sc = CommSparkContextSca.getsc();
    val list1 = Array("衣服-1","衣服-2");
    val list2 = Array("裤子-1","裤子-2");

    val rdd1 = for (elem <- sc.parallelize(list1).cartesian(sc.parallelize(list2)).collect()) {
      System.out.println(elem)
    }

  }
}
