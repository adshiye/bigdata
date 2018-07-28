package com.adshiye.bigdata.util

import java.text.{ParseException, SimpleDateFormat}
import java.util.{Calendar, Date}

import org.slf4j.LoggerFactory

object DateUtils {
  val logger = LoggerFactory.getLogger(DateUtils.getClass)

  /**
    * 判断日期是否是当前日期的几天以前或几天以后  (格式不符合返回 true)
    * @param dateStr  日期字符串
    * @param format  造型字符串  yyyyMMdd
    * @param days   与今天相隔的天数
    */
  def checkDateBefore(dateStr : String,format: String ,days : Int) : Boolean= {
    try{
      val sdf = new SimpleDateFormat(format)
      val date = sdf.parse(dateStr)
      val now_date = sdf.parse(sdf.format(new Date))
      if(days >= 0){
        if (date.getTime - now_date.getTime >= days * 24 * 60 * 60 * 1000)
          return true
      }else{
        if (date.getTime - now_date.getTime <= days * 24 * 60 * 60 * 1000)
          return true
      }
      false
    }catch {
      case e : Exception =>{
        logger.error(s"日期入参 : '${dateStr}' Error Message : ${e.getMessage}")
        true
      }
    }
  }
  //今天以后 包含今天
  def checkDateBefore0(dateStr : String ) : Boolean = {
    checkDateBefore(dateStr,"yyyyMMdd",0)
  }
  //今天以前
  def checkDateBefore(dateStr : String ) : Boolean = {
    checkDateBefore(dateStr,"yyyyMMdd",-1)
  }
  //7天以前
  def checkDateBefore7(dateStr : String ) : Boolean = {
    checkDateBefore(dateStr,"yyyyMMdd",-8)
  }

  //获取  当前时段的 分钟+秒数
  def getHourSec(hour: String): Int = try {
    val sdf = new SimpleDateFormat("HH:mm:ss")
    val dd = sdf.parse(hour)
    val cd = Calendar.getInstance
    cd.setTime(dd)
    val minute = cd.get(Calendar.MINUTE)
    val second = cd.get(Calendar.SECOND)
    minute * 60 + second
  } catch {
    case ex: Exception =>
      logger.error("获取时间段出错")
      throw ex
  }

  //获取日期的前几天
  def getDateBefore(d : String) : String = {
    val c = Calendar.getInstance()
    try {
      val date = new SimpleDateFormat("yyyyMMdd").parse(d)
      c.setTime(date);
      val day=c.get(Calendar.DATE);
      c.set(Calendar.DATE,day-1);

      val dayBefore=new SimpleDateFormat("yyyyMMdd").format(c.getTime());
      dayBefore;
    }catch {
      case ex : Exception => {
        logger.error("日期格式错误 yyyyMMdd")
        throw ex
      }
    }
  }

  //传入日期 判断日期是否是未来的，如果是未来就返回当天日期
  def filterTomorrowDate(dateStr: String): String = try {
    var str = ""
    str = dateStr.replace("-","")
    val sdf = new SimpleDateFormat("yyyyMMdd")
    val date = sdf.parse(str)
    val nowDate = new Date
    if (date.getTime - nowDate.getTime > 0) sdf.format(nowDate)
    else str
  } catch {
    case ex: ParseException =>
      logger.error(s"传入日期错误 $dateStr")
      throw ex
  }

  //根据日期获取年月周数组
  def getYearMonWeek(dateStr: String): Array[String] = {
    val sdf = new SimpleDateFormat("yyyyMMdd")
    try {
      val date = sdf.parse(dateStr)
      val calendar = Calendar.getInstance
      calendar.setTime(date)
      val year = calendar.get(Calendar.YEAR)
      val month = calendar.get(Calendar.MONTH) + 1
      val weekOfYear = calendar.get(Calendar.WEEK_OF_YEAR)
      Array[String](String.valueOf(year), String.valueOf(month), String.valueOf(weekOfYear))
    } catch {
      case ex: ParseException =>
        logger.error(s"传入日期错误 $dateStr")
        throw ex
    }
  }

  //根据时间获取时段
  def getHourPart(hour: String): Int = try {
    val sdf = new SimpleDateFormat("HH:mm:ss")
    val dd = sdf.parse(hour)
    val cd = Calendar.getInstance
    cd.setTime(dd)
    cd.get(Calendar.HOUR_OF_DAY) + 1
  } catch {
    case ex: Exception =>
      logger.error("获取时间段出错")
      1
  }

  //判断日期是否是以前的日期
  def checkIsOverYesterday(dateStr: String): Boolean = try {
    val sdf = new SimpleDateFormat("yyyyMMdd")
    val date = sdf.parse(dateStr)
    val nowDate = new Date
    if (nowDate.getTime - date.getTime >= 24 * 60 * 60 * 1000) false
    else true
  } catch {
    case ex: ParseException =>
      logger.error("operate_date传入格式不正确(yyyyMMdd)")
      false
  }

  def main(args: Array[String]): Unit = {
    println(getDateBefore("20171201"))
    println("2018-01-11".replace("-",""))
  }


}
