package kfk.spark.sparkSql


/**
  * 持久化  ：  cache   persist
  * 创建临时视图 ： createTempView  createOrReplaceTempView
  * 获取执行计划 ： explain
  * 查看schema : printSchema
  * 写数据到外部的数据存储系统  :  write
  * ds 与 df之间的转化  as   toDF
  */
object BasicOpertionScala {

  case class Person(name :String ,age : Long)

  def main(args: Array[String]): Unit = {

    val spark = CommSparkSessionScala.getSparkSession();
    val df  = spark.read.json(Comm.fileDirPath + "people.json");

//    df.cache();
//
//    df.createOrReplaceTempView("person");
//    val resultDF = spark.sql("select * from person a where 1=1")
//     spark.sql("select * from person a where 1=1").explain();
//
//    df.printSchema();
//
//    df.select("name").write.save("path");

    import spark.implicits._;
    val personDS = df.as[Person]
    personDS.show()
    personDS.printSchema()

    val personDF = personDS.toDF()
    personDF.show()







  }
}
