package com.dataflood.pfactorymssql

import org.apache.log4j.Logger

/**
 * Hello world!
 *
 */
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

  //  if (filename.length == 0) filename = getClass.getResource("/consumer_groups.xml").getFile

  logger.info("PFactoryMSSQL v0.1")
  println("PFactoryMSSQL v0.1")
  println("debug=" + debug)
  println("showme=" + showme)
  println("filename=" + filename)
  println("remainingopts=" + remainingopts)

}
