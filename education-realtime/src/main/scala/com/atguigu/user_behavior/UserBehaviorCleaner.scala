package com.atguigu.user_behavior

import org.apache.spark.{SparkConf, SparkContext}

/**
  * 用户行为数据清洗
  * 1.验证数据格式是否正确，切分后长度必须为17
  * 2.手机号脱敏，格式为123XXXX4567
  * 3.去掉username中带有的\n，否则导入写入hdfs时会换行
  */
object UserBehaviorCleaner {

  def main(args: Array[String]): Unit = {
    if(args.length != 2) {
      println("Usage:please input inputPath and outputPath")
      System.exit(1)
    }

    //获取输入输出路径
    val inputPath = args(0)
    val outputPath = args(1)

    val conf = new SparkConf().setAppName(this.getClass.getSimpleName)
    val sc = new SparkContext(conf)

    //通过输入路径获取rdd
    val eventRDD = sc.textFile(inputPath)

    //清洗数据，在算子中不要写大量业务逻辑，应该将逻辑封装到方法中
    eventRDD.filter( event =>
      chenkEventValid(event))
//      event.split("\t").length == 17)
      .map(event => maskPhone(event))
      .map(event => repairUserName(event))
      .coalesce(3)
      .saveAsTextFile(outputPath)

    sc.stop()
  }

  //验证字符串格式是否正确，只有切分后长度为17的才算正确
  def chenkEventValid(event: String) = {
    val fields = event.split("\t")
    fields.length == 17
  }

  //username为用户自定义的，子要存在"\n"，导致写入到HDFS就行
  def repairUserName(event : String)= {
    val fields = event.split("\t")

    //取出用户昵称
    val username = fields(1)

    //用户昵称不为空的时候替换"\n"
    if(username != null && !"".equals(username)){
      fields(1) = username.replace("\n","")
    }

    fields.mkString("\t")
  }

  //手机号部位空时脱敏
  def maskPhone(event: String)={
    val maskPhone = new StringBuilder
    val fields = event.split("\t")

    val phone = fields(9)
    if(isNullString(phone)){
      maskPhone.append(phone.substring(0,3)).append("xxxx").append(phone.substring(7, 11))
    }
    fields.mkString("\t")
  }

  //检验字符串是否为null
  def isNullString(str: String)={
    str != null && !"".equals(str)
  }

}
