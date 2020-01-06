package cn.tedu.hbase

import org.apache.hadoop.fs.shell.find.Result
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat
import org.apache.hadoop.mapreduce.Job
import org.apache.spark.{SparkConf, SparkContext}

object WriteHbase {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[3]").setAppName("writeHbase")
    val sc = new SparkContext(conf)
    //指定zookeeper主机
    sc.hadoopConfiguration.set("hbase.zookeeper.quorum","hadoop01,hadoop02,hadoop03")
    //指定端口号
    sc.hadoopConfiguration.set("hbase.zookeeper.property.clientPort","2181")
    //指定向hbase输出的表名
    sc.hadoopConfiguration.set(TableOutputFormat.OUTPUT_TABLE,"tb1")


    val job = new Job(sc.hadoopConfiguration)
    //指定写出数据时,输出key类型
    job.setOutputKeyClass(classOf[ImmutableBytesWritable])
    //指定写出数据时,输出value类型
    job.setOutputValueClass(classOf[Result])
    //指定输出的表类型
    job.setOutputFormatClass(classOf[TableOutputFormat[ImmutableBytesWritable]])

    val data = sc.makeRDD(List("1 tom 23","2 jerry 18","3 jim 25","4 jary 30"))
    //为了向hbase数据,需要RDD[String]-->RDD[]
    val hbaseRDD= data.map{line=>
      val info = line.split(" ")
      val id = info(0)
      val name = info(1)
      val age = info(2)
      //创建一个hbase的行对象
      val put = new Put(id.getBytes())
      put.add("cf1".getBytes(),"name".getBytes(),name.getBytes())
      put.add("cf1".getBytes(),"age".getBytes(),age.getBytes())

      (new ImmutableBytesWritable(),put)

    }

    hbaseRDD.saveAsNewAPIHadoopDataset(job.getConfiguration)

  }
}
