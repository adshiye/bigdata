package com.thinkive.bigdata.wangwei

import java.util.concurrent.{ExecutorService, Executors}

import com.thinkive.base.jdbc.DataRow
import com.thinkive.base.util.DateHelper
import com.thinkive.bigdata.wangwei.util.KafkaClient
import java.util.Date


object testKafka {
  def main(args: Array[String]): Unit = {



    val threadPool: ExecutorService =Executors.newFixedThreadPool(600)
    try {
      for(i <- 1 to 300){
        //threadPool.submit(new ThreadDemo("thread"+i))
        threadPool.execute(new ThreadDemo1(i))
      }
    }finally {
      threadPool.shutdown()
    }
  }
  class ThreadDemo1(i: Int) extends Runnable{
    override def run(){
      val random = new java.util.Random()
      val actionId = Array("1","1","2","2","2","2","3","3","4","5")
      val appId = Array("tk.bd.test1","tk.bd.test1","tk.bd.test1","tk.bd.test1","tk.bd.test1",
      "tk.bd.test1","tk.bd.test1","tk.bd.test1","tk.bd.test1","tk.bd.other")
      val appVersion = Array("V2.5.0.96","V2.5.0.96","V2.5.0.96","V2.5.0.96","V2.5.0.96",
        "V2.5.0.0","V2.5.0.0","V2.4.0.96","V2.3.0.96","V2.2.0.96")
      val time = Array("15:30:30","15:30:31","15:30:33","15:30:34","15:30:32",
        "15:30:30","15:30:31","15:30:33","14:30:30","14:30:30")
      val kafkaClient = KafkaClient.getInstance()
      val osType = Array("2","2","2","2","2","1","1","1","3","4")
      while(true){
          val date = DateHelper.formatDate(DateHelper.getDataDiff(new Date(),random.nextInt(90)),"yyyy-MM-dd")
          val data = new DataRow()
          val action_id = actionId(random.nextInt(10))
          val time_stamp = new Date().getTime
          data.set("action_id","1")
          data.set("table_name",appId(random.nextInt(10)))
          data.set("app_id",appId(random.nextInt(10)))
          data.set("app_version",appVersion(random.nextInt(10)))
          data.set("device_id","86427403239"+random.nextInt(10000))
          data.set("channel_code",osType(random.nextInt(10)))
          data.set("os_version",osType(random.nextInt(10)))
          data.set("os_type",osType(random.nextInt(10)))
          data.set("operate_time",time(random.nextInt(10)))
          data.set("operate_date",date)
          data.set("update_day",date)
          data.set("h5_version","96")
          data.set("system_lang","zh")
          data.set("login_no","0")
          data.set("client_id",null)
          data.set("sid","54452EC226C873E9E055000000000006")
          data.set("suid","17191760079240701539218377124" + i)
          data.set("network","4")
          data.set("user_type",null)
          data.set("device_oper_no","107")
          data.set("usage_oper_no","1")
          data.set("carrieroperator","")
          data.set("is_login","false")
          data.set("login_oper_no","1")
          data.set("stamp_id","20006441")
          data.set("time_stamp",time_stamp)
          data.set("ip","10.105.64.29")
          data.set("usage_no","24")
          data.set("wx_open_id","null")
          data.set("login_name","null")
          data.set("qq_open_id","null")
          data.set("imei","864274032398881")
          data.set("device","PIC-AL00")
          kafkaClient.send(data)
          println(new Date() +" thread" + i+ "-kafka")
          Thread.sleep(1)
      }
    }
  }
}
