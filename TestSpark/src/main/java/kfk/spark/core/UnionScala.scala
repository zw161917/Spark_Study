package kfk.spark.core

import kfk.spark.common.CommSparkContextSca

object UnionScala {
  def main(args: Array[String]): Unit = {
    val sc = CommSparkContextSca.getsc();
    val list1 = Array("cherry","herry");
    val list2 = Array("leo","ben","lili");

    val rdd1 = for (elem <- sc.parallelize(list1).union(sc.parallelize(list2)).collect()) {
      System.out.println(elem)
    }

  }

}
