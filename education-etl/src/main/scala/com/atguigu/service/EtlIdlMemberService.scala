package com.atguigu.service

import com.atguigu.bean.{IdlMember, IdlMember_Result, MemberZipper, MemberZipperResult}
import com.atguigu.dao.BdlMemberDao
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

/**
  * 制作宽表的具体实现
  */
object EtlIdlMemberService {

  //TODO Spark API的方式做join
  def importMemberUserApi(sparkSession: SparkSession)={
    import sparkSession.implicits._

    val bdlMember = BdlMemberDao.getBdlMember(sparkSession) //主表用户表
    val bdlMemberRegtype = BdlMemberDao.getBdlMemberRegType(sparkSession)
    val bdlBaseAd = BdlMemberDao.getBdlBaseAd(sparkSession)
    val bdlBaseWebsite = BdlMemberDao.getBdlBaseWebSite(sparkSession)
    val bdlPcentermemPaymoney = BdlMemberDao.getBdlPcentermemPayMoney(sparkSession)
    val bdlVipLevel: DataFrame = BdlMemberDao.getBdlVipLevel(sparkSession)

    //六张表做join，然后.select返回查询结果集，然后.as增加结构
    val result = bdlMember.join(bdlMemberRegtype, Seq("uid", "website"), "left_outer")
      .join(bdlBaseAd, Seq("ad_id", "website"), "left_outer")
      .join(bdlBaseWebsite, Seq("siteid", "website"), "left_outer")
      .join(bdlPcentermemPaymoney, Seq("uid", "website"), "left_outer")
      .join(bdlVipLevel, Seq("vip_id", "website"), "left_outer")
      .select("uid", "ad_id", "fullname", "iconurl", "lastlogin", "mailaddr", "memberlevel", "password"
        , "paymoney", "phone", "qq", "register", "regupdatetime", "unitname", "userip", "zipcode", "time", "appkey"
        , "appregurl", "bdp_uuid", "reg_createtime", "domain", "isranreg", "regsource", "regsourcename", "adname"
        , "siteid", "sitename", "siteurl", "site_delete", "site_createtime", "site_creator", "vip_id", "vip_level",
        "vip_start_time", "vip_end_time", "vip_last_modify_time", "vip_max_free", "vip_min_free", "vip_next_level"
        , "vip_operator", "website").as[IdlMember]

    result.groupByKey(item => item.uid + "_" + item.website)    //等同于group by
      .mapGroups{case(key,iters) =>
        val keys = key.split("_")
        val uid = Integer.parseInt(keys(0))
        val website = keys(1)

        //对value进行操作
        val idlMembers = iters.toList

        //等同于connt(if (paymoney>0, payment ,0))
        val paymoney = idlMembers.filter(_.paymoney != null).map(_.paymoney).reduceOption(_+_)
          .getOrElse(BigDecimal.apply(0.00)).doubleValue().toString

        //等同于first(ad_id)   分组后取第一条数据
        val ad_id = idlMembers.map(_.ad_id).head

      val fullname = idlMembers.map(_.fullname).head
      val icounurl = idlMembers.map(_.iconurl).head
      val lastlogin = idlMembers.map(_.lastlogin).head
      val mailaddr = idlMembers.map(_.mailaddr).head
      val memberlevel = idlMembers.map(_.memberlevel).head
      val password = idlMembers.map(_.password).head
      val phone = idlMembers.map(_.phone).head
      val qq = idlMembers.map(_.qq).head
      val register = idlMembers.map(_.register).head
      val regupdatetime = idlMembers.map(_.regupdatetime).head
      val unitname = idlMembers.map(_.unitname).head
      val userip = idlMembers.map(_.userip).head
      val zipcode = idlMembers.map(_.zipcode).head
      val time = idlMembers.map(_.time).head
      val appkey = idlMembers.map(_.appkey).head
      val appregurl = idlMembers.map(_.appregurl).head
      val bdp_uuid = idlMembers.map(_.bdp_uuid).head
      val reg_createtime = idlMembers.map(_.reg_createtime).head
      val domain = idlMembers.map(_.domain).head
      val isranreg = idlMembers.map(_.isranreg).head
      val regsource = idlMembers.map(_.regsource).head
      val regsourcename = idlMembers.map(_.regsourcename).head
      val adname = idlMembers.map(_.adname).head
      val siteid = idlMembers.map(_.siteid).head
      val sitename = idlMembers.map(_.sitename).head
      val siteurl = idlMembers.map(_.siteurl).head
      val site_delete = idlMembers.map(_.site_delete).head
      val site_createtime = idlMembers.map(_.site_createtime).head
      val site_creator = idlMembers.map(_.site_creator).head
      val vip_id = idlMembers.map(_.vip_id).head
      val vip_level = idlMembers.map(_.vip_level).max
      val vip_start_time = idlMembers.map(_.vip_start_time).min
      val vip_end_time = idlMembers.map(_.vip_end_time).max
      val vip_last_modify_time = idlMembers.map(_.vip_last_modify_time).max
      val vip_max_free = idlMembers.map(_.vip_max_free).head
      val vip_min_free = idlMembers.map(_.vip_min_free).head
      val vip_next_level = idlMembers.map(_.vip_next_level).head
      val vip_operator = idlMembers.map(_.vip_operator).head
      IdlMember_Result(uid, ad_id, fullname, icounurl, lastlogin, mailaddr, memberlevel, password, paymoney,
        phone, qq, register, regupdatetime, unitname, userip, zipcode, time, appkey, appregurl,
        bdp_uuid, reg_createtime, domain, isranreg, regsource, regsourcename, adname, siteid,
        sitename, siteurl, site_delete, site_createtime, site_creator, vip_id, vip_level,
        vip_start_time, vip_end_time, vip_last_modify_time, vip_max_free, vip_min_free,
        vip_next_level, vip_operator, website)
      }.show()
  }

  //TODO SQL的方式做join
  def importMember(sparkSession: SparkSession,time:String) = {
    import sparkSession.implicits._

    //查询全量数据，刷新到宽表，宽表不需要两张微信绑定表中的数据
    sparkSession.sql("select uid,first(ad_id),first(fullname),first(iconurl),first(lastlogin)," +
      "first(mailaddr),first(memberlevel),first(password),sum(cast(paymoney as decimal(10,4))),first(phone),first(qq)," +
      "first(register),first(regupdatetime),first(unitname),first(userip),first(zipcode)," +
      "first(time),first(appkey),first(appregurl),first(bdp_uuid),first(reg_createtime),first(domain)," +
      "first(isranreg),first(regsource),first(regsourcename),first(adname),first(siteid),first(sitename)," +
      "first(siteurl),first(site_delete),first(site_createtime),first(site_creator),first(vip_id),max(vip_level)," +
      "min(vip_start_time),max(vip_end_time),max(vip_last_modify_time),first(vip_max_free),first(vip_min_free),max(vip_next_level)," +
      "first(vip_operator),website from" +
      "(select a.uid,a.ad_id,a.fullname,a.iconurl,a.lastlogin,a.mailaddr,a.memberlevel," +
      "a.password,e.paymoney,a.phone,a.qq,a.register,a.regupdatetime,a.unitname,a.userip," +
      "a.zipcode,a.time,b.appkey,b.appregurl,b.bdp_uuid,b.createtime as reg_createtime,b.domain,b.isranreg,b.regsource," +
      "b.regsourcename,c.adname,d.siteid,d.sitename,d.siteurl,d.delete as site_delete,d.createtime as site_createtime," +
      "d.creator as site_creator,f.vip_id,f.vip_level,f.start_time as vip_start_time,f.end_time as vip_end_time," +
      "f.last_modify_time as vip_last_modify_time,f.max_free as vip_max_free,f.min_free as vip_min_free," +
      "f.next_level as vip_next_level,f.operator as vip_operator,a.website " +
      "from bdl.bdl_member a left join bdl.bdl_member_regtype b on a.uid=b.uid " +
      "and a.website=b.website left join bdl.bdl_base_ad c on a.ad_id=c.adid and a.website=c.website left join " +
      " bdl.bdl_base_website d on b.websiteid=d.siteid and b.website=d.website left join bdl.bdl_pcentermempaymoney e" +
      " on a.uid=e.uid and a.website=e.website left join bdl.bdl_vip_level f on e.vip_id=f.vip_id and e.website=f.website)r " +
      "group by uid,website").coalesce(3).write.mode(SaveMode.Overwrite).insertInto("idl.idl_member")
  }

  def importMemberZipper(sparkSession: SparkSession,time:String) = {
    import sparkSession.implicits._

    println(s"$time")
    //查询增量数据，为拉链表做准备
    val dayResult = sparkSession.sql("select a.uid,sum(a.paymoney) as paymoney,max(b.vip_level) as vip_level," +
      s"from_unixtime(unix_timestamp('$time','yyyyMMdd'),'yyyy-MM-dd') as start_time,'9999-12-31' as end_time,first(a.website) as website " +
      " from bdl.bdl_pcentermempaymoney a join " +
      s"bdl.bdl_vip_level b on a.vip_id=b.vip_id and a.website=b.website where a.time='$time' group by uid").as[MemberZipper]
//    dayResult.collect.take(5).foreach(println)

    //查询历史拉链表数据
    val historyResult = sparkSession.sql("select * from idl.idl_member_zipper").as[MemberZipper]

    //历史拉链表和每天的增量数据表做union
    dayResult.union(historyResult).groupByKey(item => item.uid + "_" + item.website)
      .mapGroups{ case (key,iters) =>
        val keys = key.split("_")
        val uid = keys(0)
        val website = keys(1)

        val list = iters.toList.sortBy(item => item.start_time) //对开始时间进行排序
        if(list.size > 1){
          //如果存在历史数据，需要对历史数据的end_time进行修改
          //获取历史数据的最后一条数据
          val oldLastModel = list(list.size - 2)

          //获取当前数据的最后一条数据
          val lastModel = list(list.size - 1)
          oldLastModel.end_time = lastModel.start_time

        }
        MemberZipperResult(list)

        //重组对象打散 刷新拉链表
      }.flatMap(_.list).coalesce(3).write.mode(SaveMode.Overwrite).insertInto("idl.idl_member_zipper")
  }
}
