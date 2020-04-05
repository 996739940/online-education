package com.atguigu.bean

object Models {

}
//TODO 暂时不清楚这里类的用处
//原始拉链表
case class MemberZipper(
                         uid: Int,
                         paymoney: String,
                         vip_level: String,
                         start_time: String,
                         var end_time: String,
                         website: String
                       )

case class MemberZipperResult(list: List[MemberZipper])


case class QueryResult(
                        uid: Int,
                        ad_id: Int,
                        memberlevel: String,
                        register: String,
                        appregurl: String, //注册来源url
                        regsource: String,
                        regsourcename: String,
                        adname: String,
                        siteid: String,
                        sitename: String,
                        vip_level: String,
                        paymoney: BigDecimal,
                        website: String
                      )

//宽表
case class IdlMember(
                      uid: Int,
                      ad_id: Int,
                      fullname: String,
                      iconurl: String,
                      lastlogin: String,
                      mailaddr: String,
                      memberlevel: String,
                      password: String,
                      paymoney: BigDecimal,
                      phone: String,
                      qq: String,
                      register: String,
                      regupdatetime: String,
                      unitname: String,
                      userip: String,
                      zipcode: String,
                      time: String,
                      appkey: String,
                      appregurl: String,
                      bdp_uuid: String,
                      reg_createtime: String,
                      domain: String,
                      isranreg: String,
                      regsource: String,
                      regsourcename: String,
                      adname: String,
                      siteid: String,
                      sitename: String,
                      siteurl: String,
                      site_delete: String,
                      site_createtime: String,
                      site_creator: String,
                      vip_id: String,
                      vip_level: String,
                      vip_start_time: String,
                      vip_end_time: String,
                      vip_last_modify_time: String,
                      vip_max_free: String,
                      vip_min_free: String,
                      vip_next_level: String,
                      vip_operator: String,
                      website: String
                    )

//宽表，他和IdlMember表唯一的区别就是paymoney的类型，
//需要测试bigdecimal在rdd与df和ds的转换中是否会出现精度丢失
case class IdlMember_Result(
                             uid: Int,
                             ad_id: Int,
                             fullname: String,
                             icounurl: String,
                             lastlogin: String,
                             mailaddr: String,
                             memberlevel: String,
                             password: String,
                             paymoney: String,
                             phone: String,
                             qq: String,
                             register: String,
                             regupdatetime: String,
                             unitname: String,
                             userip: String,
                             zipcode: String,
                             time: String,
                             appkey: String,
                             appregurl: String,
                             bdp_uuid: String,
                             reg_createtime: String,
                             domain: String,
                             isranreg: String,
                             regsource: String,
                             regsourcename: String,
                             adname: String,
                             siteid: String,
                             sitename: String,
                             siteurl: String,
                             site_delete: String,
                             site_createtime: String,
                             site_creator: String,
                             vip_id: String,
                             vip_level: String,
                             vip_start_time: String,
                             vip_end_time: String,
                             vip_last_modify_time: String,
                             vip_max_free: String,
                             vip_min_free: String,
                             vip_next_level: String,
                             vip_operator: String,
                             website: String
                           )