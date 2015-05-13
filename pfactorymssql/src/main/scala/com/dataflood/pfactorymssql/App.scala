package com.dataflood.pfactorymssql

import java.util.concurrent.CountDownLatch
import scala.collection.mutable.HashMap
import scala.xml.Elem
import scala.xml.XML
import org.apache.avro.Schema
import org.apache.log4j.Logger
import java.sql.Connection
import org.apache.commons.pool2.impl.GenericObjectPoolConfig
import org.apache.commons.pool2.impl.GenericObjectPool

object App extends App {
  val logger = Logger.getLogger(getClass.getName)

  val remainingopts = InputParams.parseArgs(args.toList, InputParams.pf)
  InputParams.setFile()
  InputParams.printInfo()

  //--------------------- read the config file -------------------// 
  var cfgXML = XML.loadFile(InputParams.filename)
  //--------------------- get total number threads----------------// 

  val poolSQLConnect = Pool.getSQLConnectPool(5)
  val tablesList = Config.getTablesList
  val latch = new CountDownLatch(tablesList.size * 5)
  val poolProducer = new KafkaProducerGroup(tablesList, latch)

  //  val arrayConnection = Config.getArayConnectionMSSQL(10)

  logger.info(poolSQLConnect.getNumIdle, poolSQLConnect.getNumWaiters, poolSQLConnect.getNumActive)
  //Thread.sleep(1000)

  //--------------------- get avro schemas---- -------------------//
  val schemaList: HashMap[Int, Schema] = Config.getSchemaList()

  // ---------- ShutdownHook, shutdown with ctrl+c----------------
  while (true) {
    val pp = poolProducer.producerPool2
    val ps = poolProducer.service
    
    logger.info(pp.getMaxIdle, pp.getNumWaiters, pp.getNumActive,pp.getReturnedCount)
    Thread.sleep(5000)
  }
  latch.await()
  sys addShutdownHook (shutdown)
  def shutdown() {
    poolSQLConnect.close()
    logger.info("------------SQL connection pool closed------------")
  }

}
