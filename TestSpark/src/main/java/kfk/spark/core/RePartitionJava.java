package kfk.spark.core;

import kfk.spark.common.CommSparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

public class RePartitionJava {
    public static void main(String[] args) {
        JavaSparkContext sc = CommSparkContext.getsc();

        List list = Arrays.asList("henry","chery","ben","leo","lili");
        JavaRDD rdd = sc.parallelize(list,2);
        JavaRDD mapIndexValues = rdd.mapPartitionsWithIndex(new Function2<Integer, Iterator, Iterator>() {
            @Override
            public Iterator call(Integer index, Iterator iterator) throws Exception {
                List _list = new ArrayList();
                while (iterator.hasNext()){
                    String userName = String.valueOf(iterator.next());
                    _list.add((index+1)  +"："+userName);
                }
                return _list.iterator();
            }
        },false);


        JavaRDD coalesceValues = mapIndexValues.repartition(3);
        JavaRDD mapIndexValues2 = coalesceValues.mapPartitionsWithIndex(new Function2<Integer, Iterator, Iterator>() {
            @Override
            public Iterator call(Integer index, Iterator iterator) throws Exception {
                List _list = new ArrayList();
                while (iterator.hasNext()){
                    String userName = String.valueOf(iterator.next());
                    _list.add((index+1)  +"："+userName);
                }
                return _list.iterator();
            }
        },false);

        for (Object o : mapIndexValues2.collect()) {
            System.out.println(o);
        }

    }
}
