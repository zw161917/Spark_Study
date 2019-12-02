package kfk.spark.core;

import kfk.spark.common.CommSparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Arrays;
import java.util.List;

public class UnionJava {
    public static void main(String[] args) {
        JavaSparkContext sc = CommSparkContext.getsc();
        List list1 = Arrays.asList("cherry","herry");
        List list2 = Arrays.asList("ben","leo");

        JavaRDD rdd1 = sc.parallelize(list1);
        JavaRDD rdd2 = sc.parallelize(list2);


        JavaRDD unionValues  = rdd1.union(rdd2);

        for (Object o : unionValues.collect()) {
            System.out.println(o);
        }
    }
}
