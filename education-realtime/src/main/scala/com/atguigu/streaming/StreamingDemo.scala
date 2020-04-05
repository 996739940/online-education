package com.atguigu.streaming

import java.text.SimpleDateFormat
import java.util.{Date, Properties}

import kafka.common.TopicAndPartition
import kafka.message.MessageAndMetadata
import kafka.serializer.StringDecoder
import net.ipip.ipdb.{City, CityInfo}
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka.{HasOffsetRanges, KafkaUtils, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import scalikejdbc.{ConnectionPool, DB}

/**
  * 通过checkpoint和11版本的kafka 11 版本的API创建ssc，
  * 保存偏移量到mysql中，保证消费数据和提交偏移量的事务性，做到精准一次性消费
  */
object StreamingDemo {

  //获取配置文件的参数
  private val prop = new Properties()
  prop.load(this.getClass.getClassLoader.getResourceAsStream("VipIncrementAnalysis.prop"))
  private val sdf = new SimpleDateFormat("yyyy-MM-dd")  //日期转换

  private val ipdb = new City(this.getClass.getClassLoader.getResourceAsStream("ipipfree.ipdb"))

  val driver = prop.getProperty("jdbcDriver")
  val url = prop.getProperty("jdbcUrl")
  val user = prop.getProperty("jdbcUser")
  val password = prop.getProperty("jdbcPassword")
  val duration = prop.getProperty("processingInterval")
  val brokers = prop.getProperty("brokers")
  val topic = prop.getProperty("topic")

  Class.forName(driver)
  ConnectionPool.singleton(url,user,password)

  def main(args: Array[String]): Unit = {
    //TODO 1.参数校验
    if(args.length != 1){
      println("缺少checkpoint目录的参数")
      System.exit(1)
    }

    val checkpointDir = args(0)

    //创建sparkstreamcontext的方式，设置带检查点的ssc
    val ssc = StreamingContext.getOrCreate(checkpointDir, ()=> getVipIncrementEveryDay(checkpointDir))

    //ssc开启
    ssc.start()
    ssc.awaitTermination()
  }

  def getVipIncrementEveryDay(checkpointDir: String): StreamingContext = {

    //创建ssc
    val conf = new SparkConf().setAppName(this.getClass.getSimpleName)
    val ssc = new StreamingContext(conf, Seconds(duration.toLong))

    //准备kafka相关参数
    val kafkaParam = Map(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> brokers)

    //获取偏移量,从mysql获取
    val fromOffset: Map[TopicAndPartition, Long] = DB.readOnly(session => session.list("select topic, part_id, offset from topic_offset") {
      rs => TopicAndPartition(rs.string("topic"), rs.int("part_id")) -> rs.long("offset")
    }.toMap)

    //创建流的方式：上下文，来源kafka参数，从mysql代替从zk中获得偏移量，kafka中每条消息过来后期望的类型
    val dstream: InputDStream[(String, String)] = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder, (String, String)](
      ssc,
      kafkaParam,
      fromOffset,
      (mmd: MessageAndMetadata[String, String]) => (mmd.key(), mmd.message()))


    var offsets = Array.empty[OffsetRange]

    //在操作数据之前，先把偏移量取出
    dstream.transform(
      rdd => {offsets = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
      rdd
    }).filter(filterCompleteOrderData)
      .map(getCityAndDate)

    ssc

  }

  //过滤出完成视频的数据
  def filterCompleteOrderData(msg:(String,String))={
    val a = msg._2.split("\t")
    if(a.length == 17){
      val eventType = a(15)
      "completeOrder".equals(eventType)
    }else{
      false
    }
  }

  //数据转换，返回((2019-04-03,北京),1)的数据
  def getCityAndDate(msg:(String,String)) = {
    val a = msg._2.split("\t")
    val ip = a(8)
    val eventTime = a(16).toLong

    //获取日期
    val date = new Date(eventTime * 1000)
    val eventDay = sdf.format(date)

    //获取城市
    var regionName = "未知"
    val info = ipdb.findInfo(ip,"zh")
    if(info != null){
      regionName = info.getRegionName
    }

    ((eventDay,regionName),1)

  }




}
