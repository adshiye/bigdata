package com.adshiye.bigdata.util

import org.apache.kudu.Type;

/**
  * Created by taoxf on 2018/4/11.
  */
class Column(n:String, v:Any, d:Type) extends Serializable{
  var columnName :String=n
  var value:Any =v
  var dataType:Type =d


  override def toString = s"Column(columnName=$columnName, value=$value, dataType=$dataType)"
}
