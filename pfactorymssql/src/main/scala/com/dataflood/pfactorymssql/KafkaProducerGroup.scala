package com.dataflood.pfactorymssql

import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable.HashMap
import org.apache.log4j.Logger
import org.apache.commons.pool2.impl.GenericObjectPoolConfig
import org.apache.commons.pool2.impl.GenericObjectPool
import java.util.concurrent.Executors
import java.util.concurrent.ExecutorService
import java.util.concurrent.{ BlockingQueue, LinkedBlockingQueue }

class Producer2(topic: String, a: Int) extends Runnable {
  var trNum: Int = a
  var topic_ = topic
  def run() {
  }
  def nur(topic: String, a: Int){
    println(topic + " ------------ " + a)
  }

}

class KafkaProducerGroup(tablesList: ArrayBuffer[HashMap[String, String]]) {
  protected val logger = Logger.getLogger(getClass.getName)

  tablesList.foreach { x =>
    val recordArray = x.toArray
    val topic = recordArray(1)._2
    logger.info(recordArray(1)._1 + " - " + topic)
    //val producerPool = createKafkaProducerPool("brokerList", topic)
    
    val producerPool2 = createKafkaProducerPool2("brokerList", 1)
    for (a <- 1 to 5) {
   //   producerPool2.addObject()
    }

    logger.info(producerPool2.getMaxIdle, producerPool2.getNumWaiters, producerPool2.getNumActive)
    logger.info(producerPool2.listAllObjects().toString())
    val service: ExecutorService = Executors.newCachedThreadPool()
    try {
      for (a <- 1 to 5) {
        val p = producerPool2.borrowObject()
        p.nur("topic", a)
//        println(p.topic_ + " -- " + p.trNum)
        service.submit(p)
            logger.info(producerPool2.getMaxIdle, producerPool2.getNumWaiters, producerPool2.getNumActive)

      }
    } finally {
      logger.info(producerPool2.listAllObjects().toString())
      service.shutdown()
    }
  }

  def createKafkaProducerPool2(topic: String, a: Int): GenericObjectPool[Producer2] = {
    val producerFactory = new BaseKafkaProducerAppFactory2(topic, a)
    

    val pooledProducerFactory = new PooledKafkaProducerAppFactory2(producerFactory)
    val poolConfig = {
      val c = new GenericObjectPoolConfig
      val maxNumProducers = 10
      c.setMaxTotal(maxNumProducers)
      c.setMaxIdle(maxNumProducers)
      c
    }
    new GenericObjectPool[Producer2](pooledProducerFactory, poolConfig)
  }

}
abstract class KafkaProducerAppFactory2(topic: String, a: Int)
  extends Serializable {

  def newInstance(): Producer2
}

class BaseKafkaProducerAppFactory2(topic: String, a: Int)
  extends KafkaProducerAppFactory2(topic, a) {

  override def newInstance() = new Producer2(topic, a)

}

