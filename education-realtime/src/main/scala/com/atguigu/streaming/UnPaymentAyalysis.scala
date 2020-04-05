package com.atguigu.streaming

import java.util.Properties
import kafka.common.TopicAndPartition
import kafka.message.MessageAndMetadata
import kafka.serializer.StringDecoder
import org.apache.spark.streaming.kafka.{HasOffsetRanges, KafkaUtils, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.SparkConf
import scalikejdbc.{ConnectionPool, DB}
import scalikejdbc._

/**
  * 开窗统计进入页面3次但是不购买的用户
  */
object UnPaymentAyalysis {

  // 从properties文件里获取各种参数
  val prop = new Properties()
  prop.load(this.getClass.getClassLoader().getResourceAsStream("UnPaymentAnalysis.properties"))

  // 获取jdbc相关参数
  val driver = prop.getProperty("jdbcDriver")
  val jdbcUrl =  prop.getProperty("jdbcUrl")
  val jdbcUser = prop.getProperty("jdbcUser")
  val jdbcPassword = prop.getProperty("jdbcPassword")

  // 设置批处理间隔
  val processingInterval = prop.getProperty("processingInterval").toLong

  // 获取kafka相关参数
  val brokers = prop.getProperty("brokers")
  val topic = prop.getProperty("topic")

  // 设置jdbc
  Class.forName(driver)
  // 设置连接池
  ConnectionPool.singleton(jdbcUrl, jdbcUser, jdbcPassword)

  def main(args: Array[String]): Unit = {
    //依次获取ssc，kafkaParams，offsetRange，messgeHandler
    val sparkConf = new SparkConf()
      .set("spark.streaming.stopGracefullyOnShutdown", "true")
      .setAppName(this.getClass.getSimpleName)
    val ssc = new StreamingContext(sparkConf,Seconds(processingInterval))

    val kafkaParams = Map[String, String]("metadata.broker.list" -> brokers,"enable.auto.commit" ->"false")

    val messageHandler = (mmd : MessageAndMetadata[String, String]) => (mmd.topic, mmd.message())

    //获取偏移量
    val fromOffsets = DB.readOnly { implicit session => sql"select topic, part_id, offset from unpayment_topic_offset".
      map { r =>
        TopicAndPartition(r.string(1), r.int(2)) -> r.long(3)
      }.list.apply().toMap
    }

    //获取Dstream
    val messages = KafkaUtils.createDirectStream(ssc,kafkaParams,fromOffsets,messageHandler)

    //声明临时数组保存最新的偏移量
    var offsetRanges : Array[OffsetRange] = Array.empty[OffsetRange]

    //业务计算
    messages.transform{rdd =>
      offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
      rdd
    }.filter{msg =>
      //过滤非订单的数据，且验证数据合法性
      filterCompleteOrderData(msg)
    }.map{msg =>
      //数据个数转化为(uid,1)
      transformData2Tuple(msg)
    }.reduceByKeyAndWindow((a:Int,b:Int) => a + b,Seconds(processingInterval * 4),Seconds(processingInterval * 2))
      .filter(state =>
        //只需要进入订单页3次，而且不是vip状态
        filterUnnormalOrderUser(state)
    ).foreachRDD(rdd => {
      val resultTuple = rdd.collect()

      //持久化数据和偏移量到mysql中
      DB.localTx{implicit session =>
        resultTuple.foreach(msg => {
          val uid = msg._1
          // 统计结果持久化到Mysql中
          println(msg)
          sql"""replace into unpayment_record(uid) values (${uid})""".executeUpdate().apply()
        })

        for (o <- offsetRanges) {
          println(o.topic,o.partition,o.fromOffset,o.untilOffset)
          // 保存offset
          sql"""update unpayment_topic_offset set offset = ${o.untilOffset} where topic = ${o.topic} and part_id = ${o.partition}""".update.apply()
        }
      }
    }
    )
  }

  //过滤进入订单页的相关数据，且验证数据合法性
  def filterCompleteOrderData(msg: (String, String)) = {
    val fields = msg._2.split("\t")
    //切分后长度不为17，代表数据不合法
    if(fields.length == 17){
      val eventType = msg._2.split("\t")(15)
      // 保留进入定单页
      "enterOrderPage".equals(eventType)
    }else{
      false
    }
  }

  //数据格式转换为（uid,1)
  def transformData2Tuple(msg:(String,String)) = {
    val fields = msg._2.split("\t")
    val uid = fields(0)
    (uid,1)
  }

  //过滤订单行为异常的用户
  def filterUnnormalOrderUser(state:(String,Int) )= {
    //获取用户id
    val uid = state._1

    //获取进入订单页的次数
    val cnt = state._2

    //当进入订单页大于等于3时，去业务表查询用户当前是否是vip状态
    if(cnt >= 3){
      val result = DB.readOnly(implicit session => {
          sql"""select id from vip_user where uid=${uid}"""
        .map(rs => {rs.get[Int](1)}).list().apply()
      })

      if(result.isEmpty){
        true
      }else{
        false
      }
    }else{
      false
    }
  }
}
