package com.dataflood.pfactorymssql

object InputParams {

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

  def setFile() = {
    if (filename.length == 0) filename = getClass.getResource("/producer_msg.xml").getFile
  }

  def printInfo() = {
    App.logger.info("PFactoryMSSQL v0.1")
    App.logger.info("debug=" + InputParams.debug)
    App.logger.info("showme=" + InputParams.showme)
    App.logger.info("filename=" + InputParams.filename)
  }

}