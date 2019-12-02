package kfk.spark

import org.apache.spark.{SparkConf, SparkContext}

object WordCountScala {

  def main(args: Array[String]): Unit = {

      val sparkConf   =
        new SparkConf().setAppName("wordcountApp").setMaster("local");

      val sc = new SparkContext(sparkConf);
      val lines = sc.textFile("hdfs://bigdata-pro-m01.kfk.com:9000/user/kfk/datas/wordcount.txt");
//      val wordcount = lines.flatMap(line => line.split("\t")).map(word => (word,1)).reduceByKey((x,y) => (x+y));
//
//    wordcount.foreach(_wordcount => (System.out.println(_wordcount._1 + "  :  "+_wordcount._2)));
      val words = lines.flatMap(line => line.split("\t"))
      val word = words.map(word => (word,1));
      val wordcount = word.reduceByKey((x,y) => (x + y));
      wordcount.foreach(_wordcount => (System.out.println(_wordcount._1 + "  :  "+_wordcount._2)));
  }
}
