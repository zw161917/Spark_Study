package kfk.spark.streaming;

import kfk.spark.common.CommSparkContext;
import org.apache.spark.api.java.Optional;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;

import java.util.Arrays;


public class StreaminUpdateByKeyJava {

    public static void main(String[] args) throws Exception{
        JavaStreamingContext jssc = CommSparkContext.getJssc();

        //要使用updateStateByKey算子，就必须设置一个checkpoint目录，开启checkpoint机制，
        //以便于内存数据丢失的时候，可以从checkpoint中恢复数据
        jssc.checkpoint("hdfs://bigdata-pro-m01.kfk.com:9000/user/kfk/datas/sparkCheckpoint");

        JavaReceiverInputDStream<String>  lines =
                jssc.socketTextStream("bigdata-pro-m01.kfk.com",9999);

        JavaDStream<String> words = lines.flatMap(line ->
                Arrays.asList(line.split(" ")).iterator());

        JavaPairDStream<String,Integer> pair = words.mapToPair(word -> new Tuple2<>(word, 1));

        //通过spark来维护一份每个单词的全局统计次数

        JavaPairDStream<String,Integer> wordCount =  pair.updateStateByKey((values , state) -> {
              Integer newValues = 0;
              if(state.isPresent()){
                  newValues = state.get();
              }

              for (Integer value : values){
                  newValues += value ;
              }
              return Optional.of(newValues);
        });


        wordCount.print();

        jssc.start();
        jssc.awaitTermination();

    }
}
