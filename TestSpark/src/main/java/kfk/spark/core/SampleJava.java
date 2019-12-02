package kfk.spark.core;

import kfk.spark.common.CommSparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Arrays;
import java.util.List;

public class SampleJava {
    public static void main(String[] args) {
        JavaSparkContext sc = CommSparkContext.getsc();
        List list = Arrays.asList("cherry","herry","leo","ben","lili");
        JavaRDD rdd = sc.parallelize(list);

        JavaRDD sampleValues  = rdd.sample(false,0.3);

        for (Object o : sampleValues.collect()) {
            System.out.println(o);
        }
    }
}
