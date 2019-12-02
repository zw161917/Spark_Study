package kfk.spark.core;

import kfk.spark.common.CommSparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Arrays;
import java.util.List;

public class CartesionJava {
    public static void main(String[] args) {
        JavaSparkContext sc = CommSparkContext.getsc();
        List list1 = Arrays.asList("衣服-1","衣服-2");
        List list2 = Arrays.asList("裤子-1","裤子-2");

        JavaRDD rdd1 = sc.parallelize(list1);
        JavaRDD rdd2 = sc.parallelize(list2);

        /**
         * (cheryy,ben)(cherry,leo)(herry,ben)(herry,leo)
         */
        JavaPairRDD carteValues = rdd1.cartesian(rdd2);
        for (Object o : carteValues.collect()) {

            System.out.println(o);
        }
    }
}
