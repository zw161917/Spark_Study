package kfk.spark.project4

import kfk.spark.common.CommSparkContextSca
import org.apache.spark.streaming.Seconds
object WindowHotWordScala {


  def main(args: Array[String]): Unit = {
    
     val jssc = CommSparkContextSca.getJssc() ;

    val inputStream = jssc.socketTextStream("bigdata-pro-m01.kfk.com",9999);

    val pairStream = inputStream.map(x =>(x,1))

    val windowDStream = pairStream.reduceByKeyAndWindow((x : Int,y : Int) => x + y,Seconds(60),Seconds(10))

    val finalDStream = windowDStream.transform( x => {

      val sortRDD = x.map(x => (x._2,x._1)).sortByKey(false).map(x => (x._2,x._1))

      val list = sortRDD.take(3)
          jssc.sparkContext.parallelize(list)
    })

    finalDStream.print()

    jssc.start()
    jssc.awaitTermination()
  }

}
