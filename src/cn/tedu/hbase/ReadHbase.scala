package cn.tedu.hbase

import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.{Result, Scan}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.protobuf.ProtobufUtil
import org.apache.hadoop.hbase.util.Base64
import org.apache.spark.{SparkConf, SparkContext}

object ReadHbase {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[3]").setAppName("readHbase")
    val sc = new SparkContext(conf)

    //创建Hbase的环境参数对象
    val hbaseConf=HBaseConfiguration.create()
    hbaseConf.set("hbase.zookeeper.quorum","hadoop01,hadoop02,hadoop03")
    hbaseConf.set("hbase.zookeeper.property.clientPort","2181")
    hbaseConf.set(TableInputFormat.INPUT_TABLE,"tb1")
    val resultRDD = sc.newAPIHadoopRDD(hbaseConf,classOf[TableInputFormat],classOf[ImmutableBytesWritable],classOf[Result])
    //创建Hbase的扫描对象
    val scan = new Scan()
    scan.setStartRow("2".getBytes())
    scan.setStopRow("4".getBytes())

    hbaseConf.set(TableInputFormat.SCAN,Base64.encodeBytes(ProtobufUtil.toScan(scan).toByteArray))

    resultRDD.foreach { case (key, value) =>
      val name = value.getValue("cf1".getBytes(),"name".getBytes())
      val age = value.getValue("cf1".getBytes(),"age".getBytes())
        println(new String(name)+":"+new String(age))
    }

  }

}
