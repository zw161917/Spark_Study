package kfk.spark.core;

import kfk.spark.common.CommSparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import scala.Tuple2;

import java.util.*;

public class GroupTopnJava {
    public static void main(String[] args) {
        JavaSparkContext sc = CommSparkContext.getsc();
        List list = Arrays.asList("class1 67",
                "class2 78",
                "class1 78",
                "class1 99",
                "class1 109",
                "class1 34",
                "class1 45",
                "class2 34",
                "class2 88",
                "class2 98",

                "class2 33");

        JavaRDD rdd = sc.parallelize(list);
        JavaPairRDD beginGroup = rdd.mapToPair(new PairFunction<String,String,Integer>() {
            @Override
            public Tuple2 call(String line) throws Exception {
                String _key = line.split(" ")[0];
                String _value = line.split(" ")[1];
                return new Tuple2(_key,_value);
            }
        });
        JavaPairRDD  groupValues = beginGroup.groupByKey();
        /**
         * <class1,(67,78,99,109,34,45,88)>
         *
         * <class1,(109,99,88)>
         */


        /**
         * topN算法演化过程
         * top(3)
         * null  ->67
         * 78  ->78,67 (add)
         * 99  ->99,78,67 (add)
         * 109 ->109,99,78 (add ,remove->67)
         * 34  ->109,99,78
         * 45  ->109,99,78
         * 88  ->109,99,88 (remove ->78)
         *
         * 78  ->78,67 (add)
         * 34  ->78,67,34
         */
        JavaPairRDD groupTop = groupValues.mapToPair(new PairFunction<Tuple2<String,Iterable>,String,Integer>() {
            @Override
            public Tuple2 call(Tuple2<String,Iterable> value) throws Exception {
                Iterator iterator = value._2.iterator();
                LinkedList linkedList = new LinkedList();
                while (iterator.hasNext()){
                    int _value = Integer.parseInt(String.valueOf(iterator.next()));
                    if(linkedList.size() == 0){
                        linkedList.add(_value);
                    }else{
                        for (int i = 0; i < linkedList.size();i++){
                            if(_value > (int)linkedList.get(i)){
                                linkedList.add(i,_value);
                                if(linkedList.size() > 3){
                                    linkedList.removeLast();
                                }
                                break;
                            }else{
                                if(linkedList.size() < 3){
                                    linkedList.add(_value);
                                    break;
                                }
                            }
                        }
                    }
                }
                return new Tuple2(value._1,linkedList);
            }
        });

        groupTop.foreach(new VoidFunction<Tuple2<String,List>>() {
            @Override
            public void call(Tuple2<String,List> o) throws Exception {
                System.out.println(o._1);
                List list = o._2;
                for (Object num : list){
                    System.out.println(num);
                }
            }
        });
    }
}
