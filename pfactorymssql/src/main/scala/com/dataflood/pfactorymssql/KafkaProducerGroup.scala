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

  val service: ExecutorService = Executors.newFixedThreadPool(100)
  val producerPool2: GenericObjectPool[KfProducer] = createKafkaProducerPool("test", 1, latch, { (pr) => finish(pr) })
  tablesList.foreach { x =>
    val recordArray = x.toArray
    val topic = recordArray(1)._2
    logger.info(recordArray(1)._1 + " - " + topic)
    //val producerPool = createKafkaProducerPool("brokerList", topic)

    //val producerPool2 = createKafkaProducerPool(topic, 1, latch)
    for (a <- 1 to 10) {
      producerPool2.addObject()
      val p = producerPool2.borrowObject()
      p.setParams(topic, a)
      producerPool2.returnObject(p)
    }

    logger.info(producerPool2.getMaxIdle, producerPool2.getNumWaiters, producerPool2.getNumActive)
    try {
      for (a <- 1 to 10) {
        val p = producerPool2.borrowObject()
        service.submit(p)
      }
    } finally {
      logger.info(producerPool2.listAllObjects().toString())
      //      service.shutdown()
      logger.info("------------service.shutdown()------------")

    }
  }
  def finish(pr: KfProducer) {
    if (true) {
      producerPool2.returnObject(pr)
      Thread.sleep(1000)
    }
    val p = producerPool2.borrowObject()
    service.submit(p)

  }

  def createKafkaProducerPool(topic: String, a: Int, latch: CountDownLatch, finish: (KfProducer) => Unit): GenericObjectPool[KfProducer] = {
    val producerFactory = new BaseKafkaProducerAppFactory2(topic, a, latch, finish)

    val pooledProducerFactory = new PooledKafkaProducerAppFactory2(producerFactory)
    val poolConfig = {
      val c = new GenericObjectPoolConfig
      val maxNumProducers = 100
      c.setMaxTotal(maxNumProducers)
      c.setMaxIdle(maxNumProducers)
      c
    }
    new GenericObjectPool[KfProducer](pooledProducerFactory, poolConfig)
  }

}

