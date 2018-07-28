package com.adshiye.bigdata.util

import java.nio.channels.ClosedChannelException
import java.util.Properties

import kafka.api.{OffsetRequest, PartitionOffsetRequestInfo, TopicMetadataRequest}
import kafka.common.TopicAndPartition
import kafka.consumer.SimpleConsumer
import kafka.utils.{ZKGroupTopicDirs, ZkUtils}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.ConsumerStrategies.{Assign, Subscribe}
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, KafkaUtils, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.zookeeper.ZooDefs
import org.slf4j.LoggerFactory
import scala.util.control.Breaks


abstract class BigDataStreamingController {
  protected val logger = LoggerFactory.getLogger(this.getClass)

  protected var kafka_topic : String = ""
  protected var kafka_broker : String = ""
  protected var spark_checkpoint : String = ""
  protected var spark_interval : Int = 0
  protected var app_name : String = ""
  protected var consumer_group : String = ""
  protected var zkQuorum : String = ""
  protected var not_rollback : String = ""

  protected var zk_offset_dir : String = ""
  protected var consume_mode : String = ""
  protected var run_mode : String = ""
  protected var zkTopicPath = ""
  protected var offsetRanges = Array[OffsetRange]()

  protected var offsetRangesBroadcast :Broadcast[Array[OffsetRange]]= _
  protected var kafkaStream :InputDStream[ConsumerRecord[String,String]] = null


  private var ssc : StreamingContext = _

  /**
  * author : zhuzh
  * desc : 加载配置文件
  */
  protected def loadSysConfig(fileName : String) : Properties = {
    val prop = new Properties()
    var file = fileName
    if(!fileName.endsWith(".properties")) file = s"$fileName.properties"
    val fis = this.getClass.getResourceAsStream(s"/$file")
    prop.load(fis);// 将属性文件流装载到Properties对象中
    kafka_topic = prop.getProperty("kafka_topic")
    kafka_broker = prop.getProperty("kafka_broker")
    spark_checkpoint = prop.getProperty("spark_checkpoint")
    spark_interval = prop.getProperty("spark_interval").toInt
    app_name = prop.getProperty("app_name")
    consumer_group = prop.getProperty("consumer_group")
    zkQuorum = prop.getProperty("zk_quorum")
    zk_offset_dir = prop.getProperty("zk_offset_dir")
    consume_mode = prop.getProperty("consume_mode")
    run_mode = prop.getProperty("run_mode")
    prop
  }



  /**
    * author : zhuzh
    * desc : 创建 kafka 源 dstream并取出ConsumerRecord
    */
  protected def createKafkaMetaStoreDStream() : DStream[ConsumerRecord[String,String]] ={
    this.zkQuorum = zkQuorum
    val sc = new SparkConf().setAppName(app_name)
    if("0".equals(run_mode)) sc.setMaster("local[*]")
    sc.set("spark.streaming.stopGracefullyOnShutdown","true")
    sc.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    //sc.set("spark.streaming.kafka.maxRatePerPartition","5")
    ssc = new StreamingContext(sc, Seconds(spark_interval))
    offsetRangesBroadcast = ssc.sparkContext.broadcast(this.offsetRanges)
    //ssc.checkpoint(spark_checkpoint)
    //val topicsSet = kafka_topic.split(",").toSet
    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> kafka_broker,
      "group.id" -> consumer_group,
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      //"zookeeper.connect"->zkQuorum,
      "auto.offset.reset" -> consume_mode,
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )
    val topicDirs = new ZKGroupTopicDirs(zk_offset_dir,kafka_topic)
    zkTopicPath = s"${topicDirs.consumerOffsetDir}"
    val zkClient = ZkUtils.createZkClient(zkQuorum,30000,30000)
    val children = zkClient.countChildren(zkTopicPath)
    var fromOffsets: Map[TopicPartition, Long] = Map()
    if (children > 0) {
      //##################################  获取每个partition得leader 开始  ##################################
      val blist = kafka_broker.split(",",-1)
      val topic2 = List(kafka_topic)
      val req = new TopicMetadataRequest(topic2, 0)
      var res : kafka.api.TopicMetadataResponse = null
      val loop = new Breaks
      loop.breakable{
        for(bip <- blist){
          try{
            val sub_bip = bip.substring(0,bip.length-5)
            val getLeaderConsumer = new SimpleConsumer(sub_bip, 9092, 5000, 5000, "OffsetLookup")  // 第一个参数是 kafka broker 的host，第二个是 port
            res = getLeaderConsumer.send(req)
            loop.break()
          }catch {
            case ece : ClosedChannelException =>{
              logger.error("#####  Kafka broker Error ######  "+bip+"  #####  通道异常")
            }
          }
        }
      }
      if(res == null) throw new Exception("请检查zk host 是否配置正确 ！")
      val topicMetaOption = res.topicsMetadata.headOption
      val partitions = topicMetaOption match {
        case Some(tm) =>
          tm.partitionsMetadata.map(pm => (pm.partitionId, pm.leader.get.host)).toMap[Int, String]  // 将结果转化为 partition -> leader 的映射关系
        case None =>
          Map[Int, String]()
      }
      //##################################  获取每个partition得leader 结束  ##################################
      for (i <- 0 until children) {
        val partitionOffset = zkClient.readData[String](s"${zkTopicPath}/${i}")
        val tp_tap = TopicAndPartition(kafka_topic, i)
        val tp = new TopicPartition(kafka_topic, i)
        val requestMin = OffsetRequest(Map(tp_tap -> PartitionOffsetRequestInfo(OffsetRequest.EarliestTime,1)))  // -2,1
        val consumerMin = new SimpleConsumer(partitions(i),9092,5000,5000,"getMinOffset")
        val curOffsets = consumerMin.getOffsetsBefore(requestMin).partitionErrorAndOffsets(tp_tap).offsets
        var nextOffset = partitionOffset.toLong
        if(curOffsets.length >0 && nextOffset < curOffsets.head){  //如果下一个offset小于当前的offset
          nextOffset = curOffsets.head
        }
        fromOffsets += (tp -> nextOffset)
      }
      //val messageHandler = (mmd : MessageAndMetadata[String, String]) => (mmd.topic, mmd.message())
      kafkaStream = KafkaUtils.createDirectStream[String, String](
        ssc,
        PreferConsistent,
        Assign[String, String](fromOffsets.keys.toList, kafkaParams, fromOffsets)
      )
    }else{
      kafkaStream = KafkaUtils.createDirectStream[String, String](
        ssc,
        PreferConsistent,
        Subscribe[String, String](Array(kafka_topic), kafkaParams)
      )
    }
    zkClient.close()

    kafkaStream.foreachRDD{ rdd =>
      logger.error(" rdd  id ---------" + rdd.id)
      offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
      offsetRangesBroadcast.unpersist(true)
      offsetRangesBroadcast = rdd.sparkContext.broadcast(offsetRanges)
    }
    kafkaStream
  }

  /**
  * author : zhuzh
  * desc : 创建 kafka 源 dstream并取出value
  */
  protected def createKafkaStream() : DStream[String] = {
    createKafkaMetaStoreDStream()
      .mapPartitions(DStreamUtils.mapKafkaValue)
      .persist(StorageLevel.MEMORY_AND_DISK)
  }


  /**
  * author : zhuzh
  * desc : 保存偏移量
  */
  protected def saveOffsetToZK() = {
    kafkaStream.foreachRDD{rdd=>
      val zkUpClient = ZkUtils.createZkClient(zkQuorum,30000,30000)
      for(offset <- offsetRangesBroadcast.value ){
        val zkPath = s"${zkTopicPath}/${offset.partition}"
        new ZkUtils(zkUpClient, null, false).updatePersistentPath(zkPath, offset.untilOffset.toString + "", ZooDefs.Ids.OPEN_ACL_UNSAFE)
      }
      zkUpClient.close()
    }
  }



  /**
  * author : zhuzh
  * desc : 启动  并  阻塞
  */
  /**
    * update by adshiye
    * desc : 捕捉程序运行的异常,回滚偏移量的修改
    */
  protected def startAndAwait(): Unit = {
    ssc.start()
    try{
      ssc.awaitTermination()
    }catch {
      case  e: Exception => {
        logger.error("println this batch run fail:" + e)
        ssc.stop(false)
        throw new Exception
      }
    }
  }



  /**
  * author : zhuzh
  * desc : 获取ssc
  */
  protected def getStreamingContext() : StreamingContext = ssc

}
