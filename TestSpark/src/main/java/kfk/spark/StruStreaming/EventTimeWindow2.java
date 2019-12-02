package kfk.spark.StruStreaming;

import kfk.spark.sparkSql.CommSparkSessionJava;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.*;
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder;
import org.apache.spark.sql.streaming.StreamingQuery;
import scala.Tuple2;

import java.sql.Date;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;

/**
 *  数据源：2019-03-29 16:40:00,hadoop
 *        2019-03-29 16:40:10,storm
 *
 */
public class EventTimeWindow2 {

    public static void main(String[] args)throws Exception {
        SparkSession spark = CommSparkSessionJava.getSparkSession() ;

        Dataset<String> df_line = spark
                .readStream()
                .format("socket")
                .option("host", "bigdata-pro-m01.kfk.com")
                .option("port", 9999)
                .load().as(Encoders.STRING());

        Dataset<EventData> df_eventdata = df_line.map( (MapFunction<String, EventData>) x -> {
            String[] lines  = x.split(",");
            return new EventData(Timestamp.valueOf(lines[0]),lines[1]);
        }, ExpressionEncoder.javaBean(EventData.class)) ;

        Dataset<Row> windowedCounts = df_eventdata.groupBy(
                functions.window(df_eventdata.col("wordtime"),
                        "10 minutes",
                        "5 minutes"),
                df_eventdata.col("word")
        ).count();

        StreamingQuery query = windowedCounts.writeStream()
                .outputMode("update")
                .format("console")
                .option("truncate","false")
                .start();
        query.awaitTermination();

    }
}
