package com.dataflood.cfactory

import java.util.Properties
import java.util.concurrent.{ CountDownLatch, TimeUnit }
import java.io.File
import org.apache.avro.Schema
import scala.collection.mutable.HashMap
import scala.xml.XML
import scala.xml.Elem
import org.apache.log4j.Logger

object CFactory {
  protected val logger = Logger.getLogger(getClass.getName)

  val usage = """
Usage: parser [-v] [-f file] [-s sopt] ...
Where: -v   Run verbosely
       -f F Set input file to F
       -s S Set Show option to S
"""

  var schema_list: HashMap[Int, Schema] = null // = SchemaListObj.list
  var cfg_XML: Elem = null

  var filename: String = ""
  var showme: String = ""
  var debug: Boolean = false
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

    //--------------------- read the config file -------------------// 
    cfg_XML = XML.loadFile(filename)

    //--------------------- get avro schemas---- -------------------// 
    schema_list = Configurations.getSchemaList()

    //--------------------- get total number threads----------------// 
    val latch = new CountDownLatch(Configurations.getThread_number())

    //--------------------- get a list of consumer groups-----------// 
    val groupList = Configurations.getcons_groupList()

    //--------------------- get a list of consumer's properties-----//
    
    val cg_GlobalConfig = Configurations.getcons_GlobalConfig()

    Configurations.getConnectMSSQL
    //--------------------- run consumer groups---------------------// 

    groupList.foreach { n =>
      val cg = new ConsumerGroup(
        n("thread_number").toString().toInt,
        n("zkconnect").toString(),
        n("groupId").toString(),
        n("topic").toString(),
        n("batch_count").toString(),
        n("topic_type").toString(),
        latch,
        cg_GlobalConfig).launch
    }
    latch.await()

    endOfJob("all threads are finished! ")
  }

}