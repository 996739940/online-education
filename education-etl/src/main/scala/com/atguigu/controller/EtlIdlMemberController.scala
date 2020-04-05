package com.atguigu.controller

import com.atguigu.service.{EtlDataService, EtlIdlMemberService}
import com.atguigu.util.HiveUtil
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

/**
  * 数据清洗的接口，将hdfs中的数据清洗后加载到bdl数据明细层
  */
object EtlIdlMemberController {
  def main(args: Array[String]): Unit = {
    //获取上下文对象和hive支持
    val sparkConf = new SparkConf().setAppName("EtlIdlMemberController")
      .setMaster("local[*]")

    val sparkSession = SparkSession.builder().config(sparkConf).enableHiveSupport().getOrCreate()
    val ssc = sparkSession.sparkContext

//    sparkSession.sql("")  //封装方法，统一管理
    HiveUtil.openDynamicPartition(sparkSession) //开启动态分区
    HiveUtil.openCompression(sparkSession)//开启压缩
    HiveUtil.useSnappyCompression(sparkSession)//使用snappy压缩

//    EtlIdlMemberService.importMember(sparkSession,"20190712")//根据用户信息聚合数据从bdl导入到idl做宽表
    EtlIdlMemberService.importMemberZipper(sparkSession,"20190712") //拉链表

  }
}
