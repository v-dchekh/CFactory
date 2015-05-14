package com.dataflood.pfactorymssql

import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable.HashMap
import org.apache.log4j.Logger
import org.apache.commons.pool2.impl.GenericObjectPoolConfig
import org.apache.commons.pool2.impl.GenericObjectPool
import java.util.concurrent.Executors
import java.util.concurrent.ExecutorService
import java.util.concurrent.{ BlockingQueue, LinkedBlockingQueue }
import java.util.concurrent.ScheduledExecutorService
import java.util.concurrent.TimeUnit
import java.util.concurrent.CountDownLatch

class KafkaProducerGroup(tablesList: ArrayBuffer[HashMap[String, String]], latch: CountDownLatch) {
  protected val logger = Logger.getLogger(getClass.getName)

  val service: ExecutorService = Executors.newFixedThreadPool(10)
  val producerPool2: GenericObjectPool[KfProducer] = Pool.createKafkaProducerPool("test", 1, latch, { (pr) => finish(pr) })
  tablesList.foreach { x =>
    val recordArray = x.toArray
    val topic = recordArray(1)._2
    logger.info(recordArray(1)._1 + " - " + topic)
    for (a <- 1 to 3) {
      producerPool2.addObject()
      val p = producerPool2.borrowObject()
      p.setParams(topic, a)
      producerPool2.returnObject(p)
    }

    logger.info(producerPool2.getMaxIdle, producerPool2.getNumWaiters, producerPool2.getNumActive)
    while (true) {
      try {
        for (a <- 1 to 3) {
          val p = producerPool2.borrowObject()
          service.submit(p)
        }
        logger.info(producerPool2.getMaxIdle, producerPool2.getNumWaiters, producerPool2.getNumActive)
        Thread.sleep(10000)
      } finally {
        logger.info(producerPool2.listAllObjects().toString())
        //      service.shutdown()
        logger.info("------------service.shutdown()------------")

      }
    }
  }
  def finish(pr: KfProducer) {
    if (pr.rowNumber == 0) {
      producerPool2.returnObject(pr)
      //Thread.sleep(1000)
      logger.info("------------------------")
    } else {
      service.submit(pr)
    }

  }

}

