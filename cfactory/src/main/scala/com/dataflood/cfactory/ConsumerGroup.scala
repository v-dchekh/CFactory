package com.dataflood.cfactory

import java.util.Properties
import java.util.concurrent.CountDownLatch
import kafka.utils.Logging
import org.apache.avro.Schema
import org.apache.log4j.Logger
import scala.collection.mutable.HashMap
import scala.collection.mutable.ArrayBuffer

class ConsumerGroup(topicCfg: Map[String, Any],
                    latch: CountDownLatch,
                    arrayConsPing: Array[MyConsumer[String]]) {

  //  info("setup:start topic=%s for zk=%s and groupId=%s".format(topic, a_zookeeper, a_groupId))
  protected val logger = Logger.getLogger(getClass.getName)
  val threadNumber: Int = topicCfg("thread_number").toString().toInt
  val zookeeper: String = topicCfg("zkconnect").toString()
  val groupId: String = topicCfg("groupId").toString()
  val topic: String = topicCfg("topic").toString()
  val batchCount: String = topicCfg("batch_count").toString()
  val topicType: String = topicCfg("topic_type").toString()

  def createConsumerConfig: Properties = {
    val props = Configurations.getcons_GlobalConfig()
    props.setProperty("group.id", groupId)
    props.setProperty("topic", topic)
    props.setProperty("batch_count", batchCount)
    props.setProperty("topic_type", topicType)
    props
  }

  def launch {
    val cg_config = createConsumerConfig
    for (i <- 1 to threadNumber by 1) {
      CFactory.threadNumberGlobal += 1
      val trnumGlobal = CFactory.threadNumberGlobal
      try {
        val myConsumer = new MyConsumer[String](trnumGlobal, latch, cg_config, trnumGlobal)
        arrayConsPing(trnumGlobal - 1) = myConsumer
        new Thread(myConsumer).start()
        logger.info(s"started global trN#: $trnumGlobal , groupId : $groupId, thread : $i")
      } catch {
        case e: Throwable =>
          if (true) logger.error("Error creating new Consumer: ", e)
          else throw e
      }
    }
  }
}