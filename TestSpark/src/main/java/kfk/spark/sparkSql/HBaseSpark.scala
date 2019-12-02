package kfk.spark.sparkSql

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapred.TableOutputFormat
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.mapred.JobConf
import org.apache.spark.sql.SparkSession

object HBaseSpark {

  def main(args: Array[String]): Unit = {

     val spark = CommSparkSessionScala.getSparkSession();
     val hbaseConf = HBaseConfiguration.create();
      hbaseConf.set("hbase.zookeeper.property.clientPort","2181");
      hbaseConf.set("hbase.zookeeper.quorum","bigdata-pro-m03.kfk.com");
      hbaseConf.set("hbase.master","bigdata-pro-m03.kfk.com:60000");
     // getHBase(hbaseConf , spark);
    writeHBase(hbaseConf,spark);

  }



  def getHBase(hbaseConf : Configuration,spark : SparkSession) : Unit = {
    hbaseConf.set(TableInputFormat.INPUT_TABLE,"stu_person");
    val hbaseRDD = spark.sparkContext.newAPIHadoopRDD(hbaseConf,classOf[TableInputFormat],
      classOf[org.apache.hadoop.hbase.io.ImmutableBytesWritable],
      classOf[org.apache.hadoop.hbase.client.Result]);

    hbaseRDD.foreach(result => {
         val key = Bytes.toString(result._2.getRow);
      val deptid = Bytes.toString(result._2.getValue("info".getBytes(),"deptid".getBytes));
      val name = Bytes.toString(result._2.getValue("info".getBytes(),"name".getBytes));
      val salary = Bytes.toString(result._2.getValue("info".getBytes(),"salary".getBytes));
      println("row key:="+key + " deptid="+deptid + " name="+name +" salary="+salary);
    })

  }


  def writeHBase(hbaseConf : Configuration,spark : SparkSession) : Unit = {

       val  jobConf =  new JobConf(hbaseConf);
    jobConf.setOutputFormat(classOf[TableOutputFormat]);
    jobConf.set(TableOutputFormat.OUTPUT_TABLE,"stu_person");

    val array = Array("002,dept-1,cherry,3000",
                      "003,dept-2,heryy,6000",
                      "004,dept-2,leo,1900");

    val rdd = spark.sparkContext.makeRDD(array);

    val saveRDD =  rdd.map(line => line.split(",")).map(x => {

      val put = new Put(Bytes.toBytes(x(0)));
      put.addColumn(Bytes.toBytes("info"),Bytes.toBytes("deptid"),Bytes.toBytes(x(1)));
      put.addColumn(Bytes.toBytes("info"),Bytes.toBytes("name"),Bytes.toBytes(x(2)));
      put.addColumn(Bytes.toBytes("info"),Bytes.toBytes("salary"),Bytes.toBytes(x(3)));
      (new ImmutableBytesWritable,put)
    });

    saveRDD.saveAsHadoopDataset(jobConf);
  }



}
