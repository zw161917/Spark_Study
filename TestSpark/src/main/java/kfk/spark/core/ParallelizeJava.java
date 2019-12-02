package kfk.spark.core;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;

import java.util.Arrays;
import java.util.List;

public class ParallelizeJava {
    public static void main(String[] args) {
        SparkConf sparkConf = new SparkConf().setAppName("parallelize").setMaster("local");

        JavaSparkContext sc = new JavaSparkContext(sparkConf);

        List list = Arrays.asList(1,2,3,4);

        JavaRDD values = sc.parallelize(list);

        Object num = values.reduce(new Function2<Integer,Integer,Integer>() {
            @Override
            public Integer call(Integer v1, Integer v2) throws Exception {
                return v1 + v2;
            }
        });

        System.out.println(num);

    }
}
