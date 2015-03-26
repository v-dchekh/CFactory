package com.dataflood.cfactory

import scala.collection.mutable.ArrayBuffer
import org.apache.avro.Schema
import java.io.File
import scala.collection.mutable.HashMap
import scala.xml.Elem
import java.util.Properties
import java.sql.DriverManager
import java.sql.Connection

object Configurations {

  def getConnectMSSQL = {
    val url = "jdbc:sqlserver://EPBYMINW2224;databaseName=DemoDB;user=scala;password=111111;"

    val driver = "com.microsoft.sqlserver.jdbc.SQLServerDriver"
    val username = "scala"
    val password = "111111"

    // there's probably a better way to do this
    var connection: Connection = null

    try {
      // make the connection
      //Class.forName("com.microsoft.sqlserver.jdbc.SQLServerDriver")
      val connection = DriverManager.getConnection(url)
      // create the statement, and run the select query
      val statement = connection.createStatement()
      val resultSet = statement.executeQuery("SELECT top(1) userid, username  from user_")
      while (resultSet.next()) {
        val userid = resultSet.getString("userid")
        val username = resultSet.getString("username")
        println("host, user = " + userid + ", " + username)
      }
    } catch {
      case e: Throwable =>
        if (true) println(e)
        else throw e
    }
  }

  def getSchemaPath(cfg_XML: Elem = CFactory.cfg_XML) = ((cfg_XML \\ "schemas") \ "@path").text

  def getSchemaList(cfg_XML: Elem = CFactory.cfg_XML) = {
    val schema_path = getSchemaPath()
    val schema_list_XML = (cfg_XML \\ "schemas" \\ "schema")
    var schema_list_Map = new HashMap[Int, Schema]
    schema_list_XML.foreach { n =>
      val schema = Schema.parse(new File(schema_path + (n \ "@file").text))
      val schema_id = (n \ "@id").text.toInt
      schema_list_Map += (schema_id -> schema)
    }
    //println("getSchemaList(cfg_XML: Elem)")
    schema_list_Map
  }

  def getcons_groupList(cfg_XML: Elem = CFactory.cfg_XML) = {
    val cons_groupList = (cfg_XML \\ "consumer_groups" \\ "consumer_group")
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

  def getThread_number(cfg_XML: Elem = CFactory.cfg_XML) = {
    val cons_groupList = (cfg_XML \\ "consumer_groups" \\ "consumer_group")
    var thread_number: Int = 0
    cons_groupList.foreach { n => thread_number += ((n \ "@thread_number").text).toInt }
    thread_number
  }

  def getcons_GlobalConfig(cfg_XML: Elem = CFactory.cfg_XML) = {
    val cons_propertiesList = (cfg_XML \\ "consumer_config" \\ "property")
    val props = new Properties()
    cons_propertiesList.foreach { n =>
      props.put((n \ "@name").text, (n \ "@value").text)
    }
    props
  }

}