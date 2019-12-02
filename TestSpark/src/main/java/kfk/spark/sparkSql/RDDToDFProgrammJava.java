package kfk.spark.sparkSql;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.ArrayList;
import java.util.List;

public class RDDToDFProgrammJava {
    public static void main(String[] args) {
        SparkSession spark = CommSparkSessionJava.getSparkSession();

        List<StructField> fields  = new ArrayList<StructField>();

        StructField structField_name = DataTypes.createStructField("name",
                DataTypes.StringType,true);

        StructField structField_age = DataTypes.createStructField("age",
                DataTypes.LongType,true);
        fields.add(structField_name);
        fields.add(structField_age);

        StructType scheme = DataTypes.createStructType(fields);


        String path = Comm.fileDirPath + "people.txt";
        JavaRDD<Row>  personRDD = spark.read().textFile(path).javaRDD().map(line -> {
                String[] lines = line.split(",");
                return RowFactory.create(lines[0],Long.valueOf(lines[1].trim()));
                });
        Dataset<Row> personDF = spark.createDataFrame(personRDD,scheme);
        personDF.createOrReplaceTempView("person");
        Dataset<Row> resultDF = spark.sql("select * from person a where a.age > 20");

        for (Row row : resultDF.javaRDD().collect()) {
            System.out.println(row);
        }

    }
}
