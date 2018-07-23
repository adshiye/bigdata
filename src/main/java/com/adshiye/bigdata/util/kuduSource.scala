package com.adshiye.bigdata.util

import org.apache.spark.sql.SparkSession
import org.apache.kudu.spark.kudu._
import org.apache.kudu.client._
import collection.JavaConverters._

object kuduSource extends App{

  val spark = SparkSession
    .builder()
    .appName("Spark SQL example").master("local")
    .getOrCreate()
  val config = Map(
    "kudu.master" -> "study-cdh-001",
    "kudu.table" -> "impala::default_app.t_bs_app_startup"
  )
  val df = spark.sqlContext.read.options(config).kudu
  println(df.rdd.partitions.length)
  df.show(10)
  spark.stop()
}
