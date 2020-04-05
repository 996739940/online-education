package com.atguigu.service

import com.atguigu.bean.QueryResult
import com.atguigu.dao.IdlMemberDao
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.{Dataset, SaveMode, SparkSession}

object AdlMemberService {

  //统计各项指标，使用api，使用api的方式则sql只需要写基本的查询语句即可，
  //查询出来结果后对返回的dataFrame进行rdd操作，常用的操作就是
  //mappartition->map->groupByKey->mapValues+reduceGroups / mapGroup
  def queryDetailApi(sparkSession: SparkSession): Unit = {
    import sparkSession.implicits._

    //TODO 对比sparkSql直接查
    //！！！cast(paymoney as decimal(10,4)) as paymoney
//    select appregurl,website,uid,from idl.idl_member
//    select appregurl,website,count(uid) from idl.idl_member group by appregurl,website
    val result: Dataset[QueryResult] = IdlMemberDao.queryIdlMemberData(sparkSession).as[QueryResult]

    result.cache()

    //统计注册来源url人数
    result.mapPartitions(partition => {
      partition.map(item => (item.appregurl + "_" + item.website , 1))
    }).groupByKey(_._1)
      //
      .mapValues(item => item._2).reduceGroups(_+_)
      .map(item => {
        val keys = item._1.split("_")
        val appregurl = keys(0)
        val website = keys(1)
        (appregurl,item._2,website)
      }).toDF("appregurl","num","website").coalesce(1)
      .write.mode(SaveMode.Overwrite).insertInto("adl.appregurl_num")

    //统计所属网站人数
    result.mapPartitions(partiton => {
      partiton.map(item => (item.sitename + "_" + item.website, 1))
    }).groupByKey(_._1).mapValues((item => item._2)).reduceGroups(_ + _)
      .map(item => {
        val keys = item._1.split("_")
        val sitename = keys(0)
        val website = keys(1)
        (sitename, item._2, website)
      }).toDF("sitename", "num", "website").coalesce(1).write.mode(SaveMode.Overwrite).insertInto("adl.sitename_num")
    ////统计所属来源人数 pc mobile wechat app
    result.mapPartitions(partition => {
      partition.map(item => (item.regsourcename + "_" + item.website, 1))
    }).groupByKey(_._1).mapValues(item => item._2).reduceGroups(_ + _)
      .map(item => {
        val keys = item._1.split("_")
        val regsourcename = keys(0)
        val website = keys(1)
        (regsourcename, item._2, website)
      }).toDF("regsourcename", "num", "website").coalesce(1).write.mode(SaveMode.Overwrite).insertInto("adl.regsourcename_num")
    //
    //统计通过各广告进来的人数
    result.mapPartitions(partition => {
      partition.map(item => (item.adname + "_" + item.website, 1))
    }).groupByKey(_._1).mapValues(_._2).reduceGroups(_ + _)
      .map(item => {
        val keys = item._1.split("_")
        val adname = keys(0)
        val website = keys(1)
        (adname, item._2, website)
      }).toDF("adname", "num", "website").coalesce(1).write.mode(SaveMode.Overwrite).insertInto("adl.adname_num")

    //统计各用户等级人数
    result.mapPartitions(partition => {
      partition.map(item => (item.memberlevel + "_" + item.website, 1))
    }).groupByKey(_._1).mapValues(_._2).reduceGroups(_ + _)
      .map(item => {
        val keys = item._1.split("_")
        val memberlevel = keys(0)
        val website = keys(1)
        (memberlevel, item._2, website)
      }).toDF("memberlevel", "num", "website").coalesce(1).write.mode(SaveMode.Overwrite).insertInto("adl.memberlevel_num")

    //统计各用户vip等级人数
    result.mapPartitions(partition => {
      partition.map(item => (item.vip_level + "_" + item.website, 1))
    }).groupByKey(_._1).mapValues(_._2).reduceGroups(_ + _)
      .map(item => {
        val keys = item._1.split("_")
        val vip_level = keys(0)
        val website = keys(1)
        (vip_level, item._2, website)
      }).toDF("vip_level", "num", "website").coalesce(1).write.mode(SaveMode.Overwrite).insertInto("adl.viplevel_num")

    //统计各memberlevel等级，支付金额前三的用户
    /*select *
      from(
        select uid,ad_id,memberlevel,register,appregurl,regsource
        ,regsourcename,adname,siteid,sitename,vip_level,cast(paymoney as decimal(10,4)),
        row_number() over (partition by website,memberlevel order by cast(paymoney as decimal(10,4)) desc) as rownum,website
        from idl.idl_member)
    where rownum<4
    order by memberlevel,rownum*/

    //TODO在spark1.6之前的版本中，无法直接使用api
    import  org.apache.spark.sql.functions._
    result.withColumn("rownum",row_number().over(
      Window.partitionBy("website","memberlevel").orderBy("paymoney"))
    )
      .where("rownum<4").orderBy("memberlevel","rownum")
      .select("uid", "memberlevel", "register", "appregurl", "regsourcename", "adname"
        , "sitename", "vip_level", "paymoney", "rownum", "website")
      .coalesce(1).write.mode(SaveMode.Overwrite).insertInto("adl.topmemberpay")

  }

  //统计各项指标，使用sql
  def queryDetailSql(sparkSession: SparkSession) = {
    val appregurlCount = IdlMemberDao.queryAppregurlCount(sparkSession)
    val siteNameCount = IdlMemberDao.querySiteNameCount(sparkSession).show()
    val regsourceNameCount = IdlMemberDao.queryRegsourceNameCount(sparkSession).show()
    val adNameCount = IdlMemberDao.queryAdNameCount(sparkSession).show()
    val memberLevelCount = IdlMemberDao.queryMemberLevelCount(sparkSession).show()
    val vipLevelCount = IdlMemberDao.queryMemberLevelCount(sparkSession).show()
    val top3MemberLevelPayMoneyUser = IdlMemberDao.getTop3MemberLevelPayMoneyUser(sparkSession).show()
  }
}
