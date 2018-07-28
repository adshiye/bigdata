package com.adshiye.bigdata

import java.util.Properties
import com.adshiye.bigdata.util._
import kafka.utils.ZkUtils
import org.apache.spark.streaming.dstream.DStream
import org.apache.zookeeper.ZooDefs

/**
  * 数据持久化到kudu
  */
object SaveKuduTest extends BigDataStreamingController {
  def main(args: Array[String]): Unit = {
    val prop: Properties = loadSysConfig("persist_saveKudu.properties")
    val kuduMaster = prop.getProperty("kudu_master")

    val sourceDStream: DStream[String] = createKafkaMetaStoreDStream().mapPartitions(DStreamUtils.mapKafkaValue)
    val jsonDS = sourceDStream.mapPartitions(DStreamUtils.mapParseObject)

    jsonDS.foreachRDD(rdd => {
      rdd.foreachPartition(partition =>{
        val kuduManager = KuduManager(kuduMaster)
        var tableName: String = ""
        var action_id: String = ""
        try{
          partition.foreach(jsonObject => {
            if(!jsonObject.containsKey("update_day") || !jsonObject.getString("update_day").isEmpty)
              jsonObject.put("update_day",jsonObject.getString("operate_date"))
            action_id = jsonObject.getString("action_id")
            tableName = jsonObject.getString("table_name")
            jsonObject.remove("table_name")
            kuduManager.insert(tableName, jsonObject)
          })
        }finally {
          kuduManager.closeKuduClient()
        }
      })
      logger.error("zkinfo---------" + this.zkQuorum + this.zkTopicPath)
      logger.error("offsetRanges---------" + offsetRangesBroadcast.value.length)
      val zkUpClient = ZkUtils.createZkClient(zkQuorum,30000,30000)
      for(offset <- offsetRangesBroadcast.value ){
        logger.error("offsetRange---------" + offset.partition + "---" + offset.fromOffset + "--" + offset.untilOffset)
        val zkPath = s"${zkTopicPath}/${offset.partition}"
        new ZkUtils(zkUpClient, null, false).updatePersistentPath(zkPath, offset.untilOffset.toString + "", ZooDefs.Ids.OPEN_ACL_UNSAFE)
      }
      zkUpClient.close()
    })
    startAndAwait()
  }
}

