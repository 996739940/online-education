package com.atguigu.dao

import org.apache.spark.sql.SparkSession

/**
  * 查询Idl层数据数据明细层数据，一张宽表
  */
object IdlMemberDao {

  //查询用户宽表数据
  def queryIdlMemberData(sparkSession: SparkSession) = {
    sparkSession.sql("select uid,ad_id,memberlevel,register,appregurl,regsource,regsourcename,adname," +
      "siteid,sitename,vip_level,cast(paymoney as decimal(10,4)) as paymoney,website from idl.idl_member ")
  }

  //暂时不清楚宽表中的数据怎么来的，但是dao层逻辑很简单，group by => count(uid)

  //统计注册来源url人数
  def queryAppregurlCount(sparkSession: SparkSession) = {
    sparkSession.sql("select appregurl,count(uid),website from idl.idl_member group by appregurl,website")
  }

  //统计所属网站人数
  def querySiteNameCount(sparkSession: SparkSession) = {
    sparkSession.sql("select sitename,count(uid),website from idl.idl_member group by sitename,website")
  }

  //统计所属来源人数
  def queryRegsourceNameCount(sparkSession: SparkSession) = {
    sparkSession.sql("select regsourcename,count(uid),website from idl.idl_member group by regsourcename,website ")
  }

  //统计通过各广告注册的人数
  def queryAdNameCount(sparkSession: SparkSession) = {
    sparkSession.sql("select adname,count(uid),website from idl.idl_member group by adname,website")
  }

  //统计各用户等级人数
  def queryMemberLevelCount(sparkSession: SparkSession) = {
    sparkSession.sql("select memberlevel,count(uid),website from idl.idl_member group by memberlevel,website")
  }

  //统计各用户vip等级人数
  def queryVipLevelCount(sparkSession: SparkSession) = {
    sparkSession.sql("select vip_level,count(uid),website from idl.idl_member group by vip_level,website")
  }

  //统计各memberlevel等级 支付金额前三的用户
  def getTop3MemberLevelPayMoneyUser(sparkSession: SparkSession) = {
    sparkSession.sql("select *from(select uid,ad_id,memberlevel,register,appregurl,regsource" +
      ",regsourcename,adname,siteid,sitename,vip_level,cast(paymoney as decimal(10,4)),row_number() over" +
      " (partition by website,memberlevel order by cast(paymoney as decimal(10,4)) desc) as rownum,website from idl.idl_member) " +
      " where rownum<4 order by memberlevel,rownum")
  }
}
