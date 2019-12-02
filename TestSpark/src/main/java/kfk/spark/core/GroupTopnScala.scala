package kfk.spark.core

import kfk.spark.common.CommSparkContextSca

object GroupTopnScala {

  def main(args: Array[String]): Unit = {
    val sc = CommSparkContextSca.getsc()
    val list = Array("class1 67",
      "class2 78",
      "class1 78",
      "class1 99",
      "class1 109",
      "class1 34",
      "class1 45",
      "class2 34",
      "class2 88",
      "class2 98",
      "class2 33");

    val rdd = sc.parallelize(list);

    val beginGroup = rdd.map( x => {
      val key = x.split(" ")(0)
      val value = x.split(" ")(1).toInt;
      (key,value)
    });

    val groupValues = beginGroup.groupByKey();
    val topValues = groupValues.map(x => {
       val values = x._2.toList.sortWith((x,y) =>  (x > y)).take(3)
      (x._1,values)
    });

    topValues.foreach(x => {
      System.out.println(x._1);
      x._2.foreach( y=> System.out.println(y))
    })
  }
}
