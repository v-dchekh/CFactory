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

  // ---------- ShutdownHook, shutdown with ctrl+c----------------
  sys addShutdownHook (shutdown)
  def shutdown() {
    poolSQLConnection.close()
    logger.info("------------SQL connection pool closed------------")
  }

  val remainingopts = InputParams.parseArgs(args.toList, InputParams.pf)
  InputParams.setFile()
  InputParams.printInfo()


  //--------------------- read the config file -------------------// 
  var cfgXML = XML.loadFile(InputParams.filename)
  //--------------------- get total number threads----------------// 

  val poolSQLConnection = Pool.getSQLConnectPool(5)
  val tablesList = Config.getTablesList
  val poolProducer = new KafkaProducerGroup(tablesList)

  val latch = new CountDownLatch(tablesList.size)
  //  val arrayConnection = Config.getArayConnectionMSSQL(10)

  logger.info(App.poolSQLConnection.getNumIdle, App.poolSQLConnection.getNumWaiters, App.poolSQLConnection.getNumActive)
  Thread.sleep(1000)

  //--------------------- get avro schemas---- -------------------//
  val schemaList: HashMap[Int, Schema] = Config.getSchemaList()

}
