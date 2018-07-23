package com.adshiye.bigdata

import org.apache.spark.sql.SparkSession
import org.apache.kudu.spark.kudu._

object testSparkTimeSpace  {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .appName("Spark SQL example").master("local")
      .getOrCreate()
    val config = Map(
      "kudu.master" -> "bd1,bd2",
      "kudu.table" -> "impala::default_app.t_bs_app_startup"
    )
    val df = spark.sqlContext.read.options(config).kudu
    println(df.rdd.partitions.length)
    df.createOrReplaceTempView("t_bs_app_startup")
    val result1 =spark.sqlContext.sql("SELECT device_id,time_stamp FROM t_bs_app_startup " +
      "WHERE operate_date > \"2018-04-12\" and operate_date < \"2018-04-14\"")
      .rdd.map(x => (x(0).toString,List((x(1).toString.toLong/(3600000)).toInt))).reduceByKey(_:::_)
    result1.foreach(x =>println(x))
    result1.mapPartitions(f =>{
        var list2 = List[(Int,Int)]()
        while (f.hasNext){
          val list1 = f.next()._2.sorted
          val list3 = List(0, 1*24, 2*24, 3*24, 4*24, 5*24, 6*24, 7*24, 8*24, 14*24, 30)
          if(list1.length == 1){
            list2=list2:+(0,1)
          }else{
            var a = 1
            for( a <- 1 until  list1.length){
              val time_span = list1(a) - list1(a-1)
              val time_span_type = list3.filter(_<=time_span).length
              list2=list2:+(time_span_type,1)
            }
          }
        }
        list2.iterator
      }).reduceByKey(_+_).foreach(x =>println(x))
    spark.stop()

  }
}
