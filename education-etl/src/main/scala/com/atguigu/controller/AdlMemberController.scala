package com.atguigu.controller

import com.atguigu.service.AdlMemberService
import com.atguigu.util.HiveUtil
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object AdlMemberController {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("AdlMemberController")
      .setMaster("local[*]")
    val sparkSession = SparkSession.builder().config(sparkConf).enableHiveSupport().getOrCreate()
    val ssc = sparkSession.sparkContext
    HiveUtil.openDynamicPartition(sparkSession)
    HiveUtil.openCompression(sparkSession)
    HiveUtil.useSnappyCompression(sparkSession)

    AdlMemberService.queryDetailApi(sparkSession)
    //    AdlMemberService.queryDetailSql(sparkSession)
  }
}
