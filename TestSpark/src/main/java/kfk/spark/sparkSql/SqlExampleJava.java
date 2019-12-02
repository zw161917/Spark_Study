package kfk.spark.sparkSql;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class SqlExampleJava {
    public static void main(String[] args) {
        SparkSession spark = SparkSession.builder()
                .appName("test")
                .master("local")
                .config("spark.sql.warehouse.dir", "/Users/caojinbo/Documents/spark/spark-warehouse")
                .getOrCreate();

        String path = "/Users/caojinbo/Documents/workspace/spark-2.4.0-old/examples/src/main/resources/people.json";
        Dataset<Row> df = spark.read().json(path);


    }
}
