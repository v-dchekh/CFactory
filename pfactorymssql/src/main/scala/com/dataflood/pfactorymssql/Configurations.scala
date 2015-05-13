package com.dataflood.pfactorymssql

import scala.collection.mutable.ArrayBuffer
import org.apache.avro.Schema
import java.io.File
import scala.collection.mutable.HashMap
import scala.xml.Elem
import java.util.Properties
import java.sql.DriverManager
import java.sql.Connection
import java.sql.Statement
import org.apache.log4j.Logger
import java.sql.ResultSet
import org.apache.commons.pool2.{ PooledObject, BasePooledObjectFactory }
import org.apache.commons.pool2.impl.{ DefaultPooledObject, GenericObjectPoolConfig, GenericObjectPool }
import java.util.concurrent.{ ExecutorService, Executors }

object Config {
  protected val logger = Logger.getLogger(getClass.getName)

  def getСonnectionStringMSSQL(cfgXML: Elem = App.cfgXML) = ((cfgXML \\ "config" \\ "sql_connect") \ "@url").text
  /*
  def getArayConnectionMSSQL(arraySize: Int = 10): Array[Connection] = {
    //    logger.info(s"Creation of SQL connection pool started (N = $arraySize)")
    val url = getСonnectionStringMSSQL()
    val driver = "com.microsoft.sqlserver.jdbc.SQLServerDriver"

    // there's probably a better way to do this
    val arrayConnection = new Array[Connection](arraySize)
    var connection: Connection = null
    var statement: Statement = null

    for (i <- 0 to arraySize - 1 by 1) {
      try {
        connection = DriverManager.getConnection(url)
        arrayConnection(i) = connection
      } catch {
        case e: Throwable =>
          if (true) println(e)
          else throw e
      }
    }
    logger.info(s"SQL connection pool created (N = $arraySize)")

    arrayConnection
  }
*/
  def getSchemaPath(cfgXML: Elem = App.cfgXML) = ((cfgXML \\ "schemas") \ "@path").text

  def getSchemaList(cfgXML: Elem = App.cfgXML) = {
    logger.info("Creation of avro schemas list started")
    val schema_path = getSchemaPath()
    val schema_list_XML = (cfgXML \\ "schemas" \\ "schema")
    var schema_list_Map = new HashMap[Int, Schema]
    schema_list_XML.foreach { n =>
      val schema = Schema.parse(new File(schema_path + (n \ "@file").text))
      val schema_id = (n \ "@id").text.toInt
      schema_list_Map += (schema_id -> schema)
    }
    //println("getSchemaList(cfgXML: Elem)")
    logger.info("Creation of avro schemas list finished")
    schema_list_Map
  }

  def getcons_groupList(cfgXML: Elem = App.cfgXML) = {
    val cons_groupList = (cfgXML \\ "consumer_groups" \\ "consumer_group")
    val groupList = new ArrayBuffer[Map[String, Any]]()

    cons_groupList.foreach { n =>
      val groupId = (n \ "@groupId").text
      val zkconnect = (n \ "@zkconnect").text
      val topic = (n \ "@topic").text
      val thread_number = ((n \ "@thread_number").text).toInt
      val batch_count = (n \ "@batch_count").text
      val topic_type = (n \ "@topic_type").text

      val m = Map(
        "groupId" -> groupId,
        "zkconnect" -> zkconnect,
        "topic" -> topic,
        "thread_number" -> thread_number,
        "batch_count" -> batch_count,
        "topic_type" -> topic_type)
      groupList += m

    }
    groupList
  }

  def getThreadNumber = {
    var thread_number: Int = 0
    val connect = App.poolSQLConnect.borrowObject()  
    try {
      try {
        var pstmt = connect.prepareCall("{? = call dbo.cdcGetTableNumber(?)}")
        pstmt.registerOutParameter(1, java.sql.Types.INTEGER);
        pstmt.registerOutParameter(2, java.sql.Types.INTEGER);
        pstmt.execute()
        val resultSet = "RETURN STATUS: " + pstmt.getInt(1)
        thread_number = pstmt.getInt(2)
        logger.debug(resultSet)
        pstmt.close

      } catch {
        case e: Throwable =>
          if (true) logger.info(e)
          else throw e
      }
    } finally {
      App.poolSQLConnect.returnObject(connect)
    }
    thread_number
  }

  def getTablesList = {
    //    val arrayConnection = getArayConnectionMSSQL(1)
    var arrayRows = new ArrayBuffer[HashMap[String, String]]()
    try {
      //val connection = arrayConnection(0)
      val connect = App.poolSQLConnect.borrowObject()
      val pstmt = connect.prepareCall("{? = call dbo.cdcGetTablesList}")
      pstmt.registerOutParameter(1, java.sql.Types.INTEGER);
      val rs = pstmt.executeQuery()
      val rsmd = rs.getMetaData()
      val cols = rsmd.getColumnCount()
      //logger.info(App.poolSQLConnection.getNumIdle, App.poolSQLConnection.getNumWaiters, App.poolSQLConnection.getNumActive)
      while (rs.next()) {
        val arrayRow = new HashMap[String, String]
        for (i <- 1 to cols) arrayRow += (rsmd.getColumnName(i) -> rs.getString(i))
        arrayRows += arrayRow
      }
      logger.debug("\n" + arrayRows.toList.mkString("\n"))
      pstmt.close
      App.poolSQLConnect.returnObject(connect)

    } catch {
      case e: Throwable =>
        if (true) logger.info(e)
        else throw e
    }

    arrayRows
  }

  def getcons_GlobalConfig(cfgXML: Elem = App.cfgXML) = {
    val cons_propertiesList = (cfgXML \\ "consumer_config" \\ "property")
    val props = new Properties()
    cons_propertiesList.foreach { n =>
      props.put((n \ "@name").text, (n \ "@value").text)
    }
    props
  }

}