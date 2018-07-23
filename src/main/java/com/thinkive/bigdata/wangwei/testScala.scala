package com.thinkive.bigdata.wangwei

object testScala {
  def main(args: Array[String]): Unit = {
    val list2 = List(("app1",List("123")),("app1",List("356")),("app2",List("356")))
    println(list2.flatMap(x => {
      Map(x._1 -> x._2)
    }))
  }
}
