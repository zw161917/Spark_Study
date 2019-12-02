package kfk.spark.core;

import kfk.spark.sparkSql.Comm;
import kfk.spark.sparkSql.CommSparkSessionJava;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class OpertionActionJava {
    public static void main(String[] args) {
        SparkSession spark = CommSparkSessionJava.getSparkSession();
        String path = Comm.fileDirPath + "people.json";

        Dataset<Row> df = spark.read().json(path);

        for (Row row : df.collectAsList()) {
            System.out.println(row);
        }

        df.first();
        for (Row row : df.takeAsList(2)) {
            System.out.println(row);
        }
    }
}
