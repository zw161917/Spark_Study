package kfk.spark.streaming



import kfk.spark.common.CommSparkContextSca
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
object StreamingKafkaScala {
  def main(args: Array[String]): Unit = {


    val  jssc = CommSparkContextSca.getJssc()
    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> "bigdata-pro-m01.kfk.com:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "use_a_separate_group_id_for_each_stream",
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> (false: java.lang.Boolean))

    val topics = Array("spark")
    val stream = KafkaUtils.createDirectStream[String, String](
      jssc,
      PreferConsistent,
      Subscribe[String, String](topics, kafkaParams))


    val words = stream.flatMap(record => record.value().trim.split(" "))

    val pair = words.map(x => (x , 1))

    val wordcount = pair.reduceByKey((x , y) => x + y)

    wordcount.print()

    jssc.start()

    jssc.awaitTermination()

  }

}
