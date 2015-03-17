package com.dataflood.cfactory

import scala.collection.mutable.ArrayBuffer
import org.apache.avro.Schema
import java.io.File
import scala.collection.mutable.HashMap
import scala.xml.Elem

object Configurations {

  def getSchemaList(cfg_XML: Elem) = {
    val schema_path = ((cfg_XML \\ "schemas") \ "@path").text
    val schema_list_XML = (cfg_XML \\ "schemas" \\ "schema")
    var schema_list_Map = new HashMap[Int, Schema]
    schema_list_XML.foreach { n =>
      val schema = Schema.parse(new File(schema_path+(n \ "@file").text))
      val schema_id = (n \ "@id").text.toInt
      schema_list_Map += (schema_id -> schema)
    }
    //println("getSchemaList(cfg_XML: Elem)")
    schema_list_Map
  }

  def getcons_groupList(cfg_XML: Elem) = {
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

  def getThread_number(cfg_XML: Elem) = {
    val cons_groupList = (cfg_XML \\ "consumer_groups" \\ "consumer_group")
    var thread_number: Int = 0
    cons_groupList.foreach { n => thread_number += ((n \ "@thread_number").text).toInt }
    thread_number
  }
}