package kfk.spark.core

import org.apache.spark.{SparkConf, SparkContext}

object SortWordCountScala {
def main(args: Array[String]): Unit = {

      val sparkConf   =
        new SparkConf().setAppName("wordcountApp").setMaster("local");

      val sc = new SparkContext(sparkConf);

      val lines = sc.textFile("hdfs://bigdata-pro-m01.kfk.com:9000/user/kfk/datas/wordcount.txt");

      val words = lines.flatMap(line => line.split("\t"))

      val word = words.map(word => (word,1));

      val wordcount = word.reduceByKey((x,y) => (x + y));

       val beginSort = wordcount.map(x => (x._2,x._1))

       val sort = beginSort.sortByKey(false)

       val sortValues = sort.map(x => (x._2,x._1))

       sortValues.foreach(_wordcount => (System.out.println(_wordcount._1 + "  :  "+_wordcount._2)));









  }
}
