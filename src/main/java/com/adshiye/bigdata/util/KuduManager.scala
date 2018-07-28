package com.adshiye.bigdata.util

import com.alibaba.fastjson.JSONObject
import org.apache.kudu.ColumnSchema
import org.apache.kudu.client._
import org.slf4j.LoggerFactory


/**
  * Created by taoxf on 2018/4/11.
  */
/**
  * Update by adshiye on 20180706
  * 写入模式改为： MANUAL_FLUSH，手动flush缓存的数据到kudu
  * 新增关闭连接方法
  * 每个线程一个session，只有在关闭session的时候，flush到kudu
  * 缓存table的连接
  * @param master
  */
case class KuduManager(master: String) extends Serializable {

  val logger = LoggerFactory.getLogger(this.getClass)
  val threadLocal: ThreadLocal[KuduSession]= new ThreadLocal[KuduSession]
  var kuduClient: KuduClient = null
  var kuduMaster: String = master
  // 缓存每个表的KuduTable对象
  val kTables  = scala.collection.mutable.Map[String, KuduTable]()

  def getKuduClient(): KuduClient = {
    if (this.kuduClient == null) {
      // 开启新连接前，清空缓存的kTables
      kTables.clear()
      kuduClient = new KuduClient.KuduClientBuilder(this.kuduMaster)
        .defaultOperationTimeoutMs(60000)
        .defaultSocketReadTimeoutMs(30000)
        .defaultAdminOperationTimeoutMs(60000)
        .build()
    }
    this.kuduClient
  }

  /**
    * 获取session,保证一个线程只有一个session
    * @return
    */
  def getSession() : KuduSession ={
    var session: KuduSession = threadLocal.get()
    if(session == null || session.isClosed){
      session = this.kuduClient.newSession()
      // 数据设置为手动刷新kudu
      session.setFlushMode(SessionConfiguration.FlushMode.MANUAL_FLUSH)
      // 设置数据缓存最大的操作数量, 每批次能够最大处理的数据量= 值 * 线程数
      session.setMutationBufferSpace(25000)
    }
    session
  }

  def getKuduTable(tableName: String): KuduTable  = {
    var kTable:  KuduTable= null
    if(kTables.contains(tableName)){
      kTable = kTables(tableName)
    }else{
      kTable = this.kuduClient.openTable(tableName)
      kTables += (tableName -> kTable)
    }
    kTable
  }

  def closeKuduClient() = {
    if (this.kuduClient != null) try
        kuduClient.close()
      catch {
        case e: KuduException =>
          logger.error("Close KuduClient Error!", e)
      }
  }

  /**
    * 关闭session,只有在关闭的时候，将数据flush到kudu
    * @return
    */
  def closeAndFlushKuduSession() = {
    val session = threadLocal.get()
    threadLocal.set(null)
    if (session != null && !session.isClosed){
      session.flush()
      try{
        session.close()
      }
      catch {
        case e: KuduException =>
          logger.error("Close KuduSession Error!", e)
      }
    }
  }

  def insert(tableName: String, jsonObject: JSONObject): Unit = {
    getKuduClient()
    val insert = createInsert(tableName, jsonObject)
    val session = getSession()
    session.apply(insert)
  }

  def createInsert(tableName: String, data: JSONObject): Insert = {
    val table = getKuduTable(tableName)
    val insert = table.newInsert()
    val row = insert.getRow
    val schema = table.getSchema
    import scala.collection.JavaConversions._
    for (colName <- data.keySet) {
      val colSchema = schema.getColumn(colName)
      fillRow(row, colSchema, data)
    }
    insert
  }

  private def fillRow(row: PartialRow, colSchema: ColumnSchema, data: JSONObject): Unit = {
    val name = colSchema.getName
    if (data.get(name) == null) return
    val kudu_type = colSchema.getType
    kudu_type match {
      case org.apache.kudu.Type.STRING =>
        row.addString(name, data.getString(name))
      case org.apache.kudu.Type.INT64 =>
      case org.apache.kudu.Type.UNIXTIME_MICROS =>
        row.addLong(name, data.getLongValue(name))
      case org.apache.kudu.Type.DOUBLE =>
        row.addDouble(name, data.getDoubleValue(name))
      case org.apache.kudu.Type.INT32 =>
        row.addInt(name, data.getIntValue(name))
      case org.apache.kudu.Type.INT16 =>
        row.addShort(name, data.getShortValue(name))
      case org.apache.kudu.Type.INT8 =>
        row.addByte(name, data.getByteValue(name))
      case org.apache.kudu.Type.BOOL =>
        row.addBoolean(name, data.getBooleanValue(name))
      case org.apache.kudu.Type.BINARY =>
        row.addBinary(name, data.getBytes(name))
      case org.apache.kudu.Type.FLOAT =>
        row.addFloat(name, data.getFloatValue(name))
      case _ =>
    }
  }
}

object kuduTest {
  def main(args: Array[String]) {
    val kuduManager = new KuduManager("bd1,bd2")
    val client = kuduManager.getKuduClient()
    print(client.toString)

  }
}