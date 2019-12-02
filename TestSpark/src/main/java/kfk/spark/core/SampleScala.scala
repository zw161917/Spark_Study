package kfk.spark.core

import kfk.spark.common.CommSparkContextSca

object SampleScala {

  def main(args: Array[String]): Unit = {
    val sc = CommSparkContextSca.getsc();
    val list = Array("cherry","herry","leo","ben","lili");
    val rdd = for (elem <- sc.parallelize(list).sample(false, 0.5).collect()) {
      System.out.println(elem)
    }
  }
}
