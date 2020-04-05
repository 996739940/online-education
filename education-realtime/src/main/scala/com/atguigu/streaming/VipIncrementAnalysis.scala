package com.atguigu.streaming

import java.sql.{Connection, Date, DriverManager}
import java.text.SimpleDateFormat
import java.util.Properties

import kafka.common.TopicAndPartition
import kafka.message.MessageAndMetadata
import kafka.serializer.StringDecoder
import net.ipip.ipdb.City
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka.{HasOffsetRanges, KafkaUtils, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import scalikejdbc.{ConnectionPool, DB, _}

/**
  * 按地区分组统计每日新增vip数量
  */
object VipIncrementAnalysis {

  // 提取出公共变量，转换算子共用
  val sdf = new SimpleDateFormat("yyyy-MM-dd")

  // 从properties文件里获取各种参数
  val prop = new Properties()
  prop.load(this.getClass.getClassLoader().getResourceAsStream("VipIncrementAnalysis.properties"))

  // 使用静态ip资源库
  val ipdb = new City(this.getClass().getClassLoader().getResource("ipipfree.ipdb").getPath())

  // 获取jdbc相关参数
  val driver = prop.getProperty("jdbcDriver")
  val jdbcUrl =  prop.getProperty("jdbcUrl")
  val jdbcUser = prop.getProperty("jdbcUser")
  val jdbcPassword = prop.getProperty("jdbcPassword")

  //  加载driver
  Class.forName(driver)
  // 设置连接池
  ConnectionPool.singleton(jdbcUrl, jdbcUser, jdbcPassword)

  def main(args: Array[String]): Unit = {
    // 参数检测
    if(args.length != 1){
      println("Usage:Please input checkpointPath")
      System.exit(1)
    }

    // 通过传入参数设置检查点
    val checkPoint = args(0)

    //创建带检查点的ssc上下文对象，因为后面需要用到他来缓存窗口数据

    val ssc = StreamingContext.getOrCreate(checkPoint,
      () => {getVipIncrementByCountry(checkPoint)}
    )

    // 启动流计算
    ssc.start()
    ssc.awaitTermination()
  }

  // 通过地区统计vip新增数量
  def getVipIncrementByCountry(checkPoint : String): StreamingContext ={
    //设置批处理间隔，streaming缓存区多长时间收集一个批次的数据
    val processingInterval = prop.getProperty("processingInterval").toLong

    //获取kafka相关参数
    val brokers = prop.getProperty("brokers")

    //设置优雅关闭streaming，通过反射设置job名称
    val sparkConf = new SparkConf()
      .set("spark.streaming.stopGracefullyOnShutdown", "true")
      .setAppName(this.getClass.getSimpleName)
    sparkConf

    val ssc = new StreamingContext(sparkConf,Seconds(processingInterval))

    //获取kafka的brokers参数和取消自动提交偏移量
    val kafkaParams = Map[String,String]("metadata.broker.list" -> brokers,"enable.auto.commit" -> "false")

    //获取offset，返回结果后封装到map中
    val fromOffsets = DB.readOnly { implicit session =>
      sql"select topic, part_id, offset from topic_offset".
        map { r =>
          TopicAndPartition(r.string(1), r.int(2)) -> r.long(3)
        }.list.apply().toMap
    }

    val messageHandler = (mmd:MessageAndMetadata[String,String]) => (mmd.topic + "-" + mmd.partition,mmd.message())

    //创建一个新的空集合保存偏移量
    var offsetRanges = Array.empty[OffsetRange]

    //获取DStreaming
    val messages = KafkaUtils.createDirectStream(ssc,kafkaParams,fromOffsets,messageHandler)

    //定义update函数
    val updateFunc = (values:Seq[Int],state:Option[Int]) => {
      //本批次value求和
      val currentCount = values.sum
      val previousCount = state.getOrElse(0)
      Some(currentCount + previousCount)
    }

    //业务计算
    messages.transform{
      //执行一切业务之前先将偏移量取出
      rdd =>
        offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
      rdd
    }.filter{msg =>
      filterCompleteOrderData(msg)
    }.map{msg =>
      getCountryAndDate(msg)
    }.updateStateByKey{
      updateFunc
    }.filter{state =>
      filter2DayBeforeState(state)
    }.foreachRDD(rdd => {
      //遍历每一个分区的数据后，将每一个分区的数据拉取到driver端
      val resultTuple = rdd.collect()

      //保存数据和offset，这样就将kafka数据的消费和提交偏移量放到了同一个事务中，保证了只有一次
      DB.localTx{implicit session =>
        resultTuple.foreach(msg => {
          val dt = msg._1._1
          val province = msg._1._2
          val cnt = msg._2.toLong

          //统计结果持久化到mysql中
          sql"""replace into vip_increment_analysis(province,cnt,dt) values (${province},${cnt},${dt})""".executeUpdate().apply()
          println(msg)
        })

        //偏移量持久化到mysql中
        for(o <- offsetRanges){
          println(o.topic,o.partition,o.untilOffset)
          sql"""update topic_offset set offset = ${o.untilOffset} where topic = ${o.topic} and part_id = ${o.partition}""".update.apply()
        }
      }
    })

    //开启检查点
    ssc.checkpoint(checkPoint)
    messages.checkpoint(Seconds(processingInterval * 10))
    ssc
  }

  //只保留最近两天的状态，因为怕系统时间活数据采集过程中有延迟，所以没有设计保留一天
  def filter2DayBeforeState(state:((String,String),Int)) = {
    //获取状态值对应的日期，并转换为13位的长整型时间戳
    val day = state._1._1
    val eventTime = sdf.parse(day).getTime

    //获取当前系统时间戳
    val currentTime = System.currentTimeMillis()

    //两者比较，保留两台内的
    if(currentTime - eventTime >= 172800000){
//      false
      true
    }else{
      true
    }
  }

  //过滤非完成订单的数据，并检验数据的合法性
  def filterCompleteOrderData(msg: (String, String)) = {
    val fields = msg._2.split("\n")
    //切分后长度不为17，代表数据不合法
    if(fields.length == 17){
      val eventType = msg._2.split("\t")(15)
      "completeOrder".equals(eventType)
    }else{
      false
    }
  }

  //数据转换，返回((2019-04-02,北京),1)格式的数据
  def getCountryAndDate(msg: (String, String)) = {

    //获取ip，获取事件事件，根据ip获取省份信息
    val fields = msg._2.split("\t")
    val ip = fields(8)
    val eventTime = fields(16).toLong
    val date = new Date(eventTime * 1000)
    val eventDay = sdf.format(date)

    var regionName = "未知"
    val info = ipdb.findInfo(ip,"CN")
    if(info != null){
      regionName = info.getRegionName()
    }

    //返回对象
    ((eventDay,regionName),1)
  }

}
