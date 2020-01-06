package cn.tedu.weblog

import java.util.Calendar

import cn.tedu.dao.{HBaseUtil, MysqlUtil}
import cn.tedu.pojo.{LogBean, Tongji}
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

/**
 * 将sparkStreaming与kafka整合,从kafka获取资源
 */

object WebLog {
  def main(args: Array[String]): Unit = {
    //如果从kafka消费,local模式的线程数至少是2个
    //其中一个线程负责SparkStreaming,另一个负责消费kafka
    val conf = new SparkConf().setMaster("local[5]").setAppName("weblog")
    val sc = new SparkContext(conf)
    val ssc = new StreamingContext(sc, Seconds(5))


    //zookeeper集群地址
    val zkHost = "hadoop01:2181,hadoop02:2181,hadoop03:2181"
    //定义消费数据组名
    val groupId = "gp1"
    //指定消费的主题信息,key是主题名称,value是消费的线程数
    //可以指定多个主题,
    val topic = Map("weblog" -> 1)
    //
    val kafkaSource = KafkaUtils.createStream(ssc,zkHost, groupId, topic).map { x => x._2 }

    kafkaSource.foreachRDD{rdd=>
      //将一批中的RDD
      val lines =rdd.toLocalIterator
      while (lines.hasNext){
        //没迭代一次获取一条数据'
        val line = lines.next()
        val info = line.split("\\|")
        val url = info(0)
        val urlname=info(1)
        val uvid = info(13)
        val ssid = info(14).split("_")(0)
        val sscount = info(14).split("_")(1)
        val sstime = info(14).split("_")(2)
        val cip = info(15)

        //用bean来封装业务字段,并写出到HBase表
        val logBean=LogBean(url,urlname,uvid,ssid,sscount,sstime,cip)
        //实时统计各个业务指标:pv uv vv newIp newCust
        //接受到一条,就算作一个pv
        val pv=1
        //难点:统计uv,uv的结果1 or 0.
        //处理思路:先获取当前访问记录的uvid,然后去HBase weblog表查询当天的数据
        //如果此uvid已经出现过,则uv=0; 反之,uv=1
        //如何查询HBase表当天的数据,我们可以指定查询的范围:
        //StartTime=当天0:00的时间戳  EndTime=sstime
        val endTime=sstime.toLong
        val calendar = Calendar.getInstance()
        //以endTime时间戳为基准,找当天的0:00的时间戳
        calendar.setTimeInMillis(endTime)
        calendar.set(Calendar.HOUR,0)
        calendar.set(Calendar.MINUTE,0)
        calendar.set(Calendar.SECOND,0)
        calendar.set(Calendar.MILLISECOND,0)

        val startTime = calendar.getTimeInMillis

        //去HBase表,使用行键正则过滤器,根据uvid来匹配查询
        val uvRegex="^\\d+_"+uvid+".*$"
        val uvResultRDD=HBaseUtil.queryByRange(sc,startTime,endTime,uvRegex)
        val uv=if(uvResultRDD.count()==0) 1 else 0

        //统计vv  vv的结果1 or 0. 根据ssid去HBase表查询当天的数据
        //处理思路同uv
        val vvRegex="^\\d+_\\d+_"+ssid+".*$"
        val vvResultRDD=HBaseUtil.queryByRange(sc,startTime,endTime,vvRegex)
        val vv=if(vvResultRDD.count()==0) 1 else 0

        //统计newIp 新增ip的结果 1 or 0 .根据当前记录中的cip 去HBase表查询历史
        //如果没有查到,则newIp=1
        val newIpRegex="^\\d+_\\d+_\\d+_"+cip+".*$"
        val newIpResultRDD=HBaseUtil.queryByRange(sc,0,endTime,newIpRegex)
        val newIp=if(newIpResultRDD.count()==0) 1 else 0

        //统计newCust 根据当前记录中的uvid,去HBase表查询历史
        //如果没有查到,则newCust=1
        val newCustRDD=HBaseUtil.queryByRange(sc,0,endTime,uvRegex)
        val newCust=if(newCustRDD.count()==0) 1 else 0

        //通过bean来封装统计好的业务指标
        val tongjiBean=Tongji(sstime.toLong,pv,uv,vv,newIp,newCust)

        //将业务指标数据插入到mysql数据库
        MysqlUtil.save(tongjiBean)
        HBaseUtil.save(sc,logBean)
      }
    }
//  kafkaSource.print()
    ssc.start()
    //--保持SparkStreaming线程一直开启
    ssc.awaitTermination()
  }
}
