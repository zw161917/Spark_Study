package kfk.spark.core;

import kfk.spark.common.CommSparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Arrays;
import java.util.List;

public class TakeSamleJava {
    public static void main(String[] args) {
        JavaSparkContext sc = CommSparkContext.getsc();
        List list = Arrays.asList("henry", "chery", "ben", "leo", "lili");
        JavaRDD rdd = sc.parallelize(list);
        List list1 = rdd.takeSample(false,3);
        for (Object o : list1) {
            System.out.println(o);
        }
    }
}
