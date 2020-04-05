package com.atguigu.util

import java.util.ResourceBundle

import com.alibaba.fastjson.JSON

object ConfigUtils {
  val condRb = ResourceBundle.getBundle("condition")
  val rb = ResourceBundle.getBundle("config")

  def getValueCondition(key:String):String = {
    val str = condRb.getString("condition.params.json")
    val jSONObject = JSON.parseObject(str)
    jSONObject.getString(key)
  }

 /* def getValueConfig(key:String):String = {
    val stream = Thread.currentThread().getContextClassLoader.getResourceAsStream("config.properties")
    val properties = new Properties()
    properties.load(stream)
    properties.getProperty(key)
  }*/

  def getValueConfig(key:String):String={
    rb.getString(key)
  }
  def main(args: Array[String]): Unit = {
//    println(ConfigUtils.getValueConfig("hive.database.test"))
    println(ConfigUtils.getValueConfig("hive.database.test"))
    println(ConfigUtils.getValueCondition("startDate"))
  }

}
