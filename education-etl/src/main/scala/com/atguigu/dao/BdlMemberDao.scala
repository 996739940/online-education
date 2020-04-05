package com.atguigu.dao

import org.apache.spark.sql.SparkSession

/**
  * 查询Idl层数据原始数据层
  */
object BdlMemberDao {
  def getBdlMember(sparkSession: SparkSession) = {
    sparkSession.sql("select uid,ad_id,email,fullname,iconurl,lastlogin,mailaddr,memberlevel," +
      "password,phone,qq,register,regupdatetime,unitname,userip,zipcode,time,website from bdl.bdl_member")
  }

  def getBdlMemberRegType(sparkSession: SparkSession) = {
    sparkSession.sql("select uid,appkey,appregurl,bdp_uuid,createtime as reg_createtime,domain,isranreg," +
      "regsource,regsourcename,websiteid as siteid,website from bdl.bdl_member_regtype ")
  }

  def getBdlBaseAd(sparkSession: SparkSession) = {
    sparkSession.sql("select adid as ad_id,adname,website from bdl.bdl_base_ad")
  }

  def getBdlBaseWebSite(sparkSession: SparkSession) = {
    sparkSession.sql("select siteid,sitename,siteurl,delete as site_delete," +
      "createtime as site_createtime,creator as site_creator,website from bdl.bdl_base_website")
  }

  def getBdlVipLevel(sparkSession: SparkSession) = {
    sparkSession.sql("select vip_id,vip_level,start_time as vip_start_time,end_time as vip_end_time," +
      "last_modify_time as vip_last_modify_time,max_free as vip_max_free,min_free as vip_min_free," +
      "next_level as vip_next_level,operator as vip_operator,website from bdl.bdl_vip_level")
  }

  //金钱有关的语句需要注意使用cast(paymoney as decimal(10,4)
  def getBdlPcentermemPayMoney(sparkSession: SparkSession) = {
    sparkSession.sql("select uid,cast(paymoney as decimal(10,4)) as paymoney,vip_id,website from bdl.bdl_pcentermempaymoney")
  }

}

