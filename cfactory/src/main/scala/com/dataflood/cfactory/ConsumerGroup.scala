package com.dataflood.cfactory

import java.util.Properties
import java.util.concurrent.CountDownLatch
import kafka.utils.Logging
import org.apache.avro.Schema
import org.apache.log4j.Logger
import scala.collection.mutable.HashMap

class ConsumerGroup(threadNumber: Int = 5,
                    zookeeper: String,
                    groupId: String,
                    topic: String,
                    batch_count: String,
                    topic_type: String,
                    latch: CountDownLatch) {

  //  info("setup:start topic=%s for zk=%s and groupId=%s".format(topic, a_zookeeper, a_groupId))
  protected val logger = Logger.getLogger(getClass.getName)

  def createConsumerConfig: Properties = {
    val props = new Properties()
    props.put("zookeeper.connect", zookeeper)
    props.put("group.id", groupId)
    props.put("zookeeper.session.timeout.ms", "4000")
    props.put("zookeeper.sync.time.ms", "200")
    props.put("auto.commit.interval.ms", "1000")
    props.put("topic", topic)
    props.put("batch_count", batch_count)
    props.put("topic_type", topic_type)
    props
  }

  def launch {
    val cg_config = createConsumerConfig

    for (i <- 1 to threadNumber by 1) {
      println("start Thread ****  groupId : " + groupId + ", thread : " + i)
      try {
        var consumer = new MyConsumer[String](":" + i, latch, cg_config)
        new Thread(consumer).start()
      } catch {
        case e: Throwable =>
          if (true) logger.error("Error creating new Consumer: ", e)
          else throw e
      }
    }
  }
}