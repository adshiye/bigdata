package com.adshiye.bigdata.util

import java.text.DecimalFormat

import com.alibaba.fastjson.{JSON, JSONObject}
import com.thinkive.base.util.StringHelper
import org.apache.kafka.clients.consumer.ConsumerRecord

import scala.util.matching.Regex

/*
DStreaming 相关运算方法
 */
object DStreamUtils extends Serializable{

  //过滤json串
  def checkJson(str : String) : Boolean = {
    try{
      if(StringHelper.isEmpty(str)){
        return false
      }
      val data = JSON.parseObject(str)
      val dateReg = new Regex("\\d{4}-\\d{2}-\\d{2}")
      val timeReg = new Regex("\\d{2}:\\d{2}:\\d{2}")
      if(StringHelper.isEmpty(data.getString("action_id"))
        || StringHelper.isEmpty(data.getString("app_id"))
        || StringHelper.isEmpty(data.getString("device_id"))
        || StringHelper.isEmpty(data.getString("suid"))
        || StringHelper.isEmpty(data.getString("operate_date"))
        || StringHelper.isEmpty(data.getString("operate_time"))
        || StringHelper.isEmpty(data.getString("time_stamp"))
        || !dateReg.pattern.matcher(data.getString("operate_date")).matches
        || !timeReg.pattern.matcher(data.getString("operate_time")).matches
      ){
        return false
      }
      return true
    }catch {
      case ex: Exception => {
        return false
      }
    }
  }

  //dbStreaming json判断
  def checkDbJson(str : String) : Boolean = {
    try{
      if(StringHelper.isEmpty(str)){
        return false
      }
      JSON.parseObject(str)
      return true
    }catch {
      case ex: Exception => {
        return false
      }
    }
  }

  //路径Streaming json判断
  //actionid 4、5 的过滤   7天之前的日期数据过滤  某些必要字段不存在过滤
  def checkTrackJson(str : String) : Boolean = {
    try{
      if(StringHelper.isEmpty(str)){
        return false
      }
      val data = JSON.parseObject(str)
      val unaction = Set("4","5")
      if(StringHelper.isEmpty(data.getString("app_id"))
        || StringHelper.isEmpty(data.getString("device_id"))
        || StringHelper.isEmpty(data.getString("suid"))
        || StringHelper.isEmpty(data.getString("action_id"))
        || unaction.contains(data.getString("action_id"))
        || DateUtils.checkDateBefore7(data.getString("operate_date").replaceAll("-",""))
      ){
        return false
      }
      true
    }catch {
      case ex: Exception => {
        false
      }
    }
  }

  //pageheatStreaming json判断
  def checkPageheatJson(str : String) : Boolean = {
    try{
      if(StringHelper.isEmpty(str)){
        return false
      }
      val data = JSON.parseObject(str)
      val dateReg = new Regex("\\d{4}-\\d{2}-\\d{2}")
      val timeReg = new Regex("\\d{2}:\\d{2}:\\d{2}")
      if(StringHelper.isEmpty(data.getString("app_id"))
        || StringHelper.isEmpty(data.getString("device_id"))
        || StringHelper.isEmpty(data.getString("suid"))
        || StringHelper.isEmpty(data.getString("action_id"))
        || StringHelper.isEmpty(data.getString("operate_date"))
        || StringHelper.isEmpty(data.getString("operate_time"))
        || !dateReg.pattern.matcher(data.getString("operate_date")).matches
        || !timeReg.pattern.matcher(data.getString("operate_time")).matches
      ){
        return false
      }
      return true
    }catch {
      case ex: Exception => {
        return false
      }
    }
  }

  def filterActionId3(jSONObject: JSONObject): Boolean ={
    try{
      val action_id = jSONObject.getString("action_id")
      if (!"3".equals(action_id))
        return false
      val app_usage_second = jSONObject.getDouble("app_usage_second")
      if(app_usage_second == null || app_usage_second>(24*60*60))
        return false
      return true
    }catch {
      case ex: Exception =>{
        return false
      }
    }
  }

  def filterActionId2(jSONObject: JSONObject): Boolean ={
    try{
      val action_id = jSONObject.getString("action_id")
      if (!"2".equals(action_id))
        return false
      val page_stay = jSONObject.getDouble("page_stay")
      if(page_stay == null || page_stay>(24*60*60) || StringHelper.isEmpty(jSONObject.getString("page_object_id")))
        return false
      return true
    }catch {
      case ex: Exception =>{
        return false
      }
    }
  }

  //修复时间 加入年月周
  def repariJsonData(iter : Iterator[JSONObject]) : Iterator[JSONObject] = {
    var res = List[JSONObject]()
    val user_type_set = Set("1","2","3","4","5","6")
    while (iter.hasNext)
    {
      val jSONObject = iter.next()
      val user_type = jSONObject.getString("user_type")
      if(!user_type_set.contains(user_type))
        jSONObject.put("user_type","-1")
      var operate_date = jSONObject.getString("operate_date")
      operate_date = DateUtils.filterTomorrowDate(operate_date)
      val ymw = DateUtils.getYearMonWeek(operate_date)
      jSONObject.put("operate_date",operate_date)
      jSONObject.put("year_part",ymw(0))
      jSONObject.put("month_part",ymw(1))
      jSONObject.put("week_part",ymw(2))
      jSONObject.put("time_part",DateUtils.getHourPart(jSONObject.getString("operate_time")))
      res = jSONObject +: res
    }
    res.iterator
  }
  def repariTrackJsonData(iter : Iterator[JSONObject]) : Iterator[JSONObject] = {
    var res = List[JSONObject]()
    val user_type_set = Set("1","2","3","4","5","6")
    while (iter.hasNext)
    {
      val jSONObject = iter.next()
      val user_type = jSONObject.getString("user_type")
      if(!user_type_set.contains(user_type))
        jSONObject.put("user_type","-1")
      var operate_date = jSONObject.getString("operate_date")
      operate_date = DateUtils.filterTomorrowDate(operate_date)
      jSONObject.put("operate_date",operate_date)
      res = jSONObject +: res
    }
    res.iterator
  }

  //过滤经纬度
  def filterLatLon(jSONObject: JSONObject): Boolean ={
    val operate_date = jSONObject.getString("operate_date")
    if(!DateUtils.checkIsOverYesterday(operate_date)){
      return false
    }
    try{
      val lat = jSONObject.getDouble("lat")//经度
      val lng = jSONObject.getDouble("lng")//纬度
      if(lat == null || lng == null || lat == 0 || lng == 0){
        return false
      }
      return true
    }catch {
      case ex: Exception => {
        return false
      }
    }
  }

  def formatLatLon(iter : Iterator[JSONObject]) : Iterator[(String,Long)] = {
    var res = List[(String,Long)]()
    while (iter.hasNext)
    {
      val jSONObject = iter.next()
      val df = new DecimalFormat("######0.0")
      val lat = jSONObject.getDouble("lat")//经度
      val lng = jSONObject.getDouble("lng")//纬度
      val operate_date = jSONObject.getString("operate_date")
      val app_id = jSONObject.getString("app_id")
      val formatLat = df.format(lat)
      val formatLng = df.format(lng)
      val key = "geo:"+operate_date+":"+app_id+":"+formatLat+":"+formatLng
      res = (key,1L) +: res
    }
    res.iterator
  }

  def mapKafkaValue(iter : Iterator[ConsumerRecord[String,String]]): Iterator[String] ={
    var res = List[String]()
    while (iter.hasNext)
    {
      val cur = iter.next.value
      res = cur +: res
    }
    res.iterator
  }

  def mapParseObject(iter : Iterator[String]): Iterator[JSONObject] ={
    var res = List[JSONObject]()
    while (iter.hasNext)
    {
      val cur = JSON.parseObject(iter.next)
      res = cur +: res
    }
    res.iterator
  }
  def mapDevice(iter : Iterator[JSONObject]): Iterator[(String,JSONObject)] ={
    var res = List[(String,JSONObject)]()
    while (iter.hasNext)
    {
      val j = iter.next()
      val cur = (s"${j.getString("suid")}&&${j.getString("app_id")}",j)
      res = cur +: res
    }
    res.iterator
  }
  def mapDeviceObj(iter : Iterator[(String,JSONObject)]): Iterator[JSONObject] ={
    var res = List[JSONObject]()
    while (iter.hasNext)
    {
      val j = iter.next()
      val cur = j._2
      res = cur +: res
    }
    res.iterator
  }

  def mapDeviceNetwork(iter : Iterator[JSONObject]): Iterator[(String,(Double,Long))] ={
    var res = List[(String,(Double,Long))]()
    while (iter.hasNext)
    {
      val j = iter.next()
      val cur = (s"${j.getString("device_id")}&&${j.getString("app_id")}&&${j.getString("operate_date")}&&${j.getString("network")}",(j.getDoubleValue("app_usage_second"),1L))
      res = cur +: res
    }
    res.iterator
  }
  def mapDeviceUsertime(iter : Iterator[JSONObject]): Iterator[(String,(Double,Int))] ={
    var res = List[(String,(Double,Int))]()
    while (iter.hasNext)
    {
      val j = iter.next()
      val cur = (s"${j.getString("device_id")}&&${j.getString("app_id")}&&${j.getString("operate_date")}",(j.getDoubleValue("app_usage_second"),1))
      res = cur +: res
    }
    res.iterator
  }
  def mapDeviceUserPart(iter : Iterator[JSONObject]): Iterator[(String,Double)] ={
    var res = List[(String,Double)]()
    while (iter.hasNext)
    {
      val j = iter.next()
      val device_id = j.getString("device_id")
      val app_id = j.getString("app_id")
      var operate_date = j.getString("operate_date")
      var time_part = j.getIntValue("time_part")
      val df = new DecimalFormat("######0.00")
      var sec1 = DateUtils.getHourSec(j.getString("operate_time"))
      var sec2 = df.parse(df.format(j.getDoubleValue("app_usage_second")-sec1)).doubleValue()
      if(sec2 > 0){//跨时段
        val cur1 = (s"${device_id}&&${app_id}&&${operate_date}&&${time_part}",sec1.toDouble)
        res = cur1 +: res
        val times = (sec2 / (60*60)).toInt
        for(i <- 0 until  times+1){
          if(time_part > 1){//不跨天
            time_part = time_part-1
            if(sec2 - 60*60 >= 0) {
              sec2 = sec2-60*60
              val cur2 = (s"${device_id}&&${app_id}&&${operate_date}&&${time_part}",(60*60).toDouble)
              res = cur2 +: res
            }else{
              val cur2 = (s"${device_id}&&${app_id}&&${operate_date}&&${time_part}",sec2)
              res = cur2 +: res
            }
          }else{//跨天
            time_part = 24
            operate_date = DateUtils.getDateBefore(operate_date)
            if(sec2 - 60*60 >= 0) {
              sec2 = sec2-60*60
              val cur2 = (s"${device_id}&&${app_id}&&${operate_date}&&${time_part}",(60*60).toDouble)
              res = cur2 +: res
            }else{
              val cur2 = (s"${device_id}&&${app_id}&&${operate_date}&&${time_part}",sec2)
              res = cur2 +: res
            }
          }
        }
      }else{
        val cur = (s"${device_id}&&${app_id}&&${operate_date}&&${time_part}",j.getDoubleValue("app_usage_second"))
        res = cur +: res
      }
    }
    res.iterator
  }

}


