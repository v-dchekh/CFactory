package com.dataflood.cfactory

import java.util.concurrent.CountDownLatch
import scala.collection.mutable.HashMap
import scala.xml.Elem
import scala.xml.XML
import org.apache.avro.Schema
import org.apache.log4j.Logger
import java.sql.Connection

object CFactory {
  protected val logger = Logger.getLogger(getClass.getName)
  // ---------- ShutdownHook, shutdown with ctrl+c----------------
  sys addShutdownHook (shutdown)
  def shutdown() {
    logger.info("stop a flush process")
    shutDownFlag = true
    Thread.sleep(6000)
    arrayConsPing.foreach { x =>
      if (x != null) x.close()
      logger.info("thread : " + x.trnumGlobal_ + " stopped, total messages consumed = " + x.numMessagesTotal)
    }
    Thread.sleep(1000)
    logger.info("------------all threads stopped------------")
  }

  val usage = """
              Usage: parser [-v] [-f file] [-s sopt] ...
              Where: -v   Run verbosely
                     -f F Set input file to F
                     -s S Set Show option to S
              """

  var schemaList: HashMap[Int, Schema] = null // = SchemaListObj.list
  var cfgXML: Elem = null
  var arrayConnection: Array[Connection] = null
  var arrayConsPing: Array[MyConsumer[String]] = null

  var filename: String = ""
  var showme: String = ""
  var debug: Boolean = false
  var threadNumberGlobal: Int = 0

  var shutDownFlag: Boolean = false
  //  var flushFlag: Boolean = false

  val unknown = "(^-[^\\s])".r

  val pf: PartialFunction[List[String], List[String]] = {
    case "-v" :: tail =>
      debug = true; tail
    case "-f" :: (arg: String) :: tail =>
      filename = arg; tail
    case "-s" :: (arg: String) :: tail =>
      showme = arg; tail
    case unknown(bad) :: tail => endOfJob("unknown argument " + bad + "\n" + usage)
  }

  def parseArgs(args: List[String], pf: PartialFunction[List[String], List[String]]): List[String] = args match {
    case Nil => Nil
    case _   => if (pf isDefinedAt args) parseArgs(pf(args), pf) else args.head :: parseArgs(args.tail, pf)
  }

  def endOfJob(msg: String = usage) = {
    println(msg)
    sys.exit(1)
  }

  def main(args: Array[String]) {
    //---------- read and parce arguments ---------------// 
    val arglist = args.toList
    val remainingopts = parseArgs(arglist, pf)

    if (filename.length == 0) filename = getClass.getResource("/consumer_groups.xml").getFile

    logger.info("CFactory v0.1")
    println("CFactory v0.1")
    println("debug=" + debug)
    println("showme=" + showme)
    println("filename=" + filename)
    println("remainingopts=" + remainingopts)

    
    //val ks :KafkaSpark = new KafkaSpark 
    
    //--------------------- read the config file -------------------// 
    cfgXML = XML.loadFile(filename)

    //--------------------- get avro schemas---- -------------------// 
    schemaList = Configurations.getSchemaList()

    //--------------------- get total number threads----------------// 
    val latch = new CountDownLatch(Configurations.getThread_number())

    //--------------------- get a list of consumer groups-----------// 
    val groupList = Configurations.getcons_groupList()

    //--------------------- get a list of consumer's properties-----//

    //    val cg_GlobalConfig = Configurations.getcons_GlobalConfig()

    //--------------------- get an arrayConnection to MS SQL ---------//
    arrayConnection = Configurations.getArayConnectionMSSQL(latch.getCount.toInt)

    //--------------------- run consumer groups---------------------// 
    arrayConsPing = new Array[MyConsumer[String]](latch.getCount.toInt)

    groupList.foreach { n =>
      val cg = new ConsumerGroup(
        n,
        latch,
        arrayConsPing).launch
    }
    logger.info("------------all threads started------------")
    //--------------------- run flush messages each 5 second-----------------//
    while (!shutDownFlag) {
      Thread.sleep(5000)
      arrayConsPing.foreach { x => x.flushOnTime }
    }
    logger.info("flush process stopped")

    latch.await()

  }

}