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


object Configurations {
  protected val logger = Logger.getLogger(getClass.getName)

  def getСonnectionStringMSSQL(cfgXML: Elem = App.cfgXML) = ((cfgXML \\ "config" \\ "sql_connect") \ "@url").text

  def getConnectMSSQL: Statement = {
    val url = getСonnectionStringMSSQL()

    val driver = "com.microsoft.sqlserver.jdbc.SQLServerDriver"

    // there's probably a better way to do this
    var connection: Connection = null
    var statement: Statement = null

    try {
      // make the connection
      //Class.forName("com.microsoft.sqlserver.jdbc.SQLServerDriver")
      connection = DriverManager.getConnection(url)
      // create the statement, and run the select query
      statement = connection.createStatement()
      /*
      val resultSet = statement.executeQuery("SELECT top(1) userid, username  from user_")
      while (resultSet.next()) {
        val userid = resultSet.getString("userid")
        val username = resultSet.getString("username")
        println("host, user = " + userid + ", " + username)
      }
      * 
      */
    } catch {
      case e: Throwable =>
        if (true) println(e)
        else throw e
    }
    statement
  }

  def getArayStatementMSSQL(arraySize: Int = 10): Array[Statement] = {
    val url = getСonnectionStringMSSQL()

    val driver = "com.microsoft.sqlserver.jdbc.SQLServerDriver"

    // there's probably a better way to do this
    var arrayStatement = new Array[Statement](arraySize)
    var connection: Connection = null
    var statement: Statement = null

    for (i <- 0 to arraySize - 1 by 1) {
      try {
        connection = DriverManager.getConnection(url)
        arrayStatement(i) = connection.createStatement()
      } catch {
        case e: Throwable =>
          if (true) println(e)
          else throw e
      }
    }
    arrayStatement
  }

  def getArayConnectionMSSQL(arraySize: Int = 10): Array[Connection] = {
    logger.info("Creation of SQL connection pool started")
    val url = getСonnectionStringMSSQL()
    val driver = "com.microsoft.sqlserver.jdbc.SQLServerDriver"

    // there's probably a better way to do this
    var arrayConnection = new Array[Connection](arraySize)
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
    logger.info("Creation of SQL connection pool finished")

    arrayConnection
  }

  def getSchemaPath(cfgXML: Elem = App.cfgXML) = ((cfgXML \\ "schemas") \ "@path").text

  def getSchemaList(cfgXML: Elem = App.cfgXML) = {
    val schema_path = getSchemaPath()
    val schema_list_XML = (cfgXML \\ "schemas" \\ "schema")
    var schema_list_Map = new HashMap[Int, Schema]
    schema_list_XML.foreach { n =>
      val schema = Schema.parse(new File(schema_path + (n \ "@file").text))
      val schema_id = (n \ "@id").text.toInt
      schema_list_Map += (schema_id -> schema)
    }
    //println("getSchemaList(cfgXML: Elem)")
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

  def getThread_number(cfgXML: Elem = App.cfgXML) = {
    val cons_groupList = (cfgXML \\ "consumer_groups" \\ "consumer_group")
    var thread_number: Int = 0
    cons_groupList.foreach { n => thread_number += ((n \ "@thread_number").text).toInt }
    thread_number
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