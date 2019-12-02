package kfk.spark.core;

import kfk.spark.common.CommSparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Arrays;
import java.util.List;

public class DistinctJava {
    public static void main(String[] args) {
        JavaSparkContext sc = CommSparkContext.getsc();
        List list1 = Arrays.asList("cherry", "herry", "leo","ben", "cherry");

        JavaRDD rdd = sc.parallelize(list1);

        JavaRDD distinctValues = rdd.distinct();

        for (Object o : distinctValues.collect()) {

            System.out.println(o);
        }

    }
}
