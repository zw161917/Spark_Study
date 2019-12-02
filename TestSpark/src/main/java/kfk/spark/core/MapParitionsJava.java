package kfk.spark.core;

import kfk.spark.common.CommSparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.util.DoubleAccumulator;

import java.util.*;

public class MapParitionsJava {

    public static void main(String[] args) {
        JavaSparkContext sc = CommSparkContext.getsc();

        List list = Arrays.asList("henry","cherry","leo","ben");
        JavaRDD rdd = sc.parallelize(list,2);
        final Map<String,Double> map = new HashMap<String,Double>();
        map.put("henry",99.4);
        map.put("cherry",79.9);
        map.put("leo",88.3);
        map.put("ben",67.5);

        JavaRDD mapPartition = rdd.mapPartitions(new FlatMapFunction<Iterator,Iterator>() {
            @Override
            public Iterator call(Iterator iterator) throws Exception {
                List list = new ArrayList();
                while (iterator.hasNext()){
                    String userName =String.valueOf(iterator.next());
                    Double score = map.get(userName).doubleValue();
                    list.add(score);
                }
                return list.iterator();
            }
        });
        mapPartition.foreach(new VoidFunction() {
            @Override
            public void call(Object o) throws Exception {
                System.out.println(o);
            }
        });


    }
}
