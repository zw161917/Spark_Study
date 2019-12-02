package kfk.spark.core;

import kfk.spark.common.CommSparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.VoidFunction;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

public class MapParitionsWithIndexJava {
    public static void main(String[] args) {
        JavaSparkContext sc = CommSparkContext.getsc();
        List list = Arrays.asList("henry","cherry","leo","ben");
        JavaRDD rdd = sc.parallelize(list,3);
        final JavaRDD indexValues = rdd.mapPartitionsWithIndex(new Function2<Integer, Iterator, Iterator>() {
            @Override
            public Iterator call(Integer index, Iterator iterator) throws Exception {
                List list = new ArrayList();
                while (iterator.hasNext()){
                    String _indexStr = iterator.next() + ":"+(index + 1);
                    list.add(_indexStr);
                }
                return list.iterator();
            }
        },false);
        indexValues.foreach(new VoidFunction() {
            @Override
            public void call(Object o) throws Exception {
                System.out.println(o);
            }
        });
    }
}
