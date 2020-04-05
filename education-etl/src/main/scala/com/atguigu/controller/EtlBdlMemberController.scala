package com.atguigu.controller

import com.atguigu.service.EtlDataService
import com.atguigu.util.HiveUtil
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

/**
  * 数据清洗的接口，将hdfs中的数据清洗后加载到bdl数据明细层
  */
object EtlBdlMemberController {
  def main(args: Array[String]): Unit = {
    //获取上下文对象和hive支持
    val sparkConf = new SparkConf().setAppName("EtlMemberController")
      .setMaster("local[*]")
    val sparkSession = SparkSession.builder().config(sparkConf).enableHiveSupport().getOrCreate()
    val ssc = sparkSession.sparkContext

    HiveUtil.openDynamicPartition(sparkSession) //开启动态分区
    HiveUtil.openCompression(sparkSession) //开启压缩
    HiveUtil.useSnappyCompression(sparkSession) //使用snappy压缩

    //对用户表原始数据进行数据清洗，存入dbl层表中
    EtlDataService.etlBaseadlog(ssc, sparkSession) //导入基础广告表数据
    EtlDataService.etlBasewebsitelog(ssc, sparkSession) //导入基础网站表数据
    EtlDataService.etlMemberlog(ssc, sparkSession) //清洗用户数据
    EtlDataService.etlMemberRegtype(ssc, sparkSession) //清洗用户注册数据
    EtlDataService.etlPcentermempaymoneylog(ssc, sparkSession) //导入用户支付情况记录
    EtlDataService.etlPcentermemviplevellog(ssc, sparkSession) //导入vip基础数据
    EtlDataService.etlWxTypeLog(ssc, sparkSession) //导入微信基础数据

//    EtlDataService.etlMemberWxboundLog(ssc, sparkSession) //导入微信绑定数据
  }
}
