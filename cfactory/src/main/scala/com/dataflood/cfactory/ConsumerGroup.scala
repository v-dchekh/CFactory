package com.dataflood.cfactory

import java.util.Properties
import java.util.concurrent.CountDownLatch
import kafka.utils.Logging
import org.apache.avro.Schema
import org.apache.log4j.Logger
import scala.collection.mutable.HashMap
import scala.collection.mutable.ArrayBuffer

class ConsumerGroup(threadNumber: Int = 5,
                    zookeeper: String,
                    groupId: String,
                    topic: String,
                    batch_count: String,
                    topic_type: String,
                    latch: CountDownLatch,
                    cg_GlobalConfig: Properties,
                    consPingArray : Array[MyConsumer[String]]) {

  //  info("setup:start topic=%s for zk=%s and groupId=%s".format(topic, a_zookeeper, a_groupId))
  protected val logger = Logger.getLogger(getClass.getName)

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
      CFactory.thread_numberGlobal = CFactory.thread_numberGlobal + 1
      var trnumGlobal = CFactory.thread_numberGlobal
      try {
        var myConsumer = new MyConsumer[String](trnumGlobal, latch, cg_config, trnumGlobal)
        consPingArray(trnumGlobal - 1) = myConsumer
        new Thread(myConsumer).start()
        logger.info(s"started global trN#: $trnumGlobal , groupId : $groupId, thread : $i" )
      } catch {
        case e: Throwable =>
          if (true) logger.error("Error creating new Consumer: ", e)
          else throw e
      }
    }
    /*
    val pingCons = consPingArray(1)
    Thread.sleep(10000)
    logger.info("before pingCons.numMessages = " + pingCons.numMessages)
    pingCons.numMessages = 0
    logger.info("after  pingCons.numMessages = " + pingCons.numMessages)
    */
  }
}