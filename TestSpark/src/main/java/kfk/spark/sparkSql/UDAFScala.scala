package kfk.spark.sparkSql

object UDAFScala {

  def main(args: Array[String]): Unit = {
    val spark = CommSparkSessionScala.getSparkSession();
    val userPath = Comm.fileDirPath + "user.json";
    spark.read.json(userPath).createOrReplaceTempView("user");
    spark.udf.register("count",new MyCount);
    spark.sql("select count(deptName),deptName from user group by deptName").show()
  }

}
