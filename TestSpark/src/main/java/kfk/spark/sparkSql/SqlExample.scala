package kfk.spark.sparkSql

import org.apache.spark.sql.SparkSession

object SqlExample {


  case class Person(name : String , age : Long)

  def main(args: Array[String]): Unit = {

      val spark = SparkSession.builder()
        .appName("test")
        .master("local")
        .config("spark.sql.warehouse.dir", "/Users/caojinbo/Documents/spark/spark-warehouse")
        .getOrCreate();


    import spark.implicits._;
    val path = "/Users/caojinbo/Documents/workspace/spark-2.4.0-old/examples/src/main/resources/people.json";
    val df = spark.read.json(path)
//    |-- age: long (nullable = true)
//    |-- name: string (nullable = true)
    //df.select("name","age").show()
//    df.select($"name",$"age" + 1).show()
//    df.filter($"age" < 20).show()
//    df.groupBy("age").count().show()

//    df.createOrReplaceTempView("test");
//
//    spark.sql("select * from test").show()

    val caseClassDS = Seq(Person("cherry",20)).toDS()
    caseClassDS.map(x => x.age + 1).show()
    caseClassDS.show()

    val commonDS = Seq(1,2,3).toDS()
    commonDS.map(x => x + 1).show()
    //commonDS.show()

    val jsonDS = spark.read.json(path).as[Person]
    jsonDS.show()

  }

}
