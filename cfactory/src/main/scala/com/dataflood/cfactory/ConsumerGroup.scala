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
  var threadNumber: Int = topicCfg("thread_number").toString().toInt
  var zookeeper: String = topicCfg("zkconnect").toString()
  var groupId: String = topicCfg("groupId").toString()
  var topic: String = topicCfg("topic").toString()
  var batch_count: String = topicCfg("batch_count").toString()
  var topic_type: String = topicCfg("topic_type").toString()

  def createConsumerConfig: Properties = {
    val props = Configurations.getcons_GlobalConfig()
    props.setProperty("group.id", groupId)
    props.setProperty("topic", topic)
    props.setProperty("batch_count", batch_count)
    props.setProperty("topic_type", topic_type)
    props
  }

  def launch {
    var cg_config = createConsumerConfig
    for (i <- 1 to threadNumber by 1) {
      CFactory.threadNumberGlobal += 1
      var trnumGlobal = CFactory.threadNumberGlobal
      try {
        var myConsumer = new MyConsumer[String](trnumGlobal, latch, cg_config, trnumGlobal)
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