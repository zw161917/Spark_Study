package kfk.spark.streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Durations, StreamingContext}

object WordCountScala {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[2]").setAppName("NetworkWordCount")

    val jssc = new StreamingContext(conf, Durations.seconds(5))

    val lines = jssc.socketTextStream("bigdata-pro-m01.kfk.com", 9999)

    val words = lines.flatMap(line => line.split(" "));

    val pair = words.map(word => (word,1))

    val wordcount = pair.reduceByKey((x , y) => x + y)

    wordcount.print()

    jssc.start()

    jssc.awaitTermination()
  }

}
