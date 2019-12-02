package kfk.spark.core;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class persistJava {
    public static JavaSparkContext getsc(){
        SparkConf sparkConf = new SparkConf().setAppName("parallelize").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(sparkConf);
        return sc;
    }
    public static void main(String[] args) {

        JavaRDD lines = getsc().textFile("hdfs://bigdata-pro-m01.kfk.com:9000/user/kfk/datas/2015082819").cache();
        long begin = System.currentTimeMillis();
        System.out.println(lines.count());
        System.out.println(System.currentTimeMillis() - begin);

        long begin1 = System.currentTimeMillis();
        System.out.println(lines.count());
        System.out.println(System.currentTimeMillis() - begin1);


    }
}
