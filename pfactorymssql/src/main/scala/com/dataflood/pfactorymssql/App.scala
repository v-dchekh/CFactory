package com.dataflood.pfactorymssql

import java.util.concurrent.CountDownLatch
import scala.collection.mutable.HashMap
import scala.xml.Elem
import scala.xml.XML
import org.apache.avro.Schema
import org.apache.log4j.Logger
import java.sql.Connection

object App extends App {
  protected val logger = Logger.getLogger(getClass.getName)

  val usage = """
              Usage: parser [-v] [-f file] [-s sopt] ...
              Where: -v   Run verbosely
                     -f F Set input file to F
                     -s S Set Show option to S
              """
  val unknown = "(^-[^\\s])".r

  var filename: String = ""
  var showme: String = ""
  var debug: Boolean = false
  var threadNumberGlobal: Int = 0

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

  val arglist = args.toList
  val remainingopts = parseArgs(arglist, pf)

  var cfgXML: Elem = null

  if (filename.length == 0) filename = getClass.getResource("/producer_msg.xml").getFile

  logger.info("PFactoryMSSQL v0.1")
  println("PFactoryMSSQL v0.1")
  println("debug=" + debug)
  println("showme=" + showme)
  println("filename=" + filename)
  println("remainingopts=" + remainingopts)

  //--------------------- read the config file -------------------// 
  cfgXML = XML.loadFile(filename)
  //--------------------- get total number threads----------------// 

  val tablesList = Config.getTablesList
  val latch = new CountDownLatch(tablesList.size)
  val arrayConnection = Config.getArayConnectionMSSQL(tablesList.size)
  
  //--------------------- get avro schemas---- -------------------//
  val schemaList: HashMap[Int, Schema] = Config.getSchemaList()
}
