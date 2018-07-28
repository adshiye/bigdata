package com.adshiye.bigdata.util.other

import org.apache.spark.sql.SparkSession

/**
  * @Description:
  * @Author: adshiye
  * @Create: 2018/07/23 16:58
  */
object kuduSource extends App{

  val spark = SparkSession
    .builder()
    .appName("Spark SQL example").master("local")
    .getOrCreate()
  val config = Map(
    "kudu.master" -> "study-cdh-001",
    "kudu.table" -> "impala::default_app.t_bs_app_startup"
  )
//  val df = spark.sqlContext.read.options(config).kudu
//  println(df.rdd.partitions.length)
//  df.show(10)
//  spark.stop()
}
