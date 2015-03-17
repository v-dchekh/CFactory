package com.dataflood

import scala.xml.XML
import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable.HashMap
import org.apache.avro.Schema
import java.io.File

class SchemaList {

  def get(file: String) = {
    var file_ = file
    if (file_.length == 0) file_ = getClass.getResource("/producer_msg.xml").getFile
    var schemas_XML = XML.loadFile(file_)
    var schemas_list = (schemas_XML \\ "schemas" \\ "schema")
    //    val schemas_array = new ArrayBuffer[Map[String, Any]]()
    var scores = new HashMap[Int, Schema]
    schemas_list.foreach { n =>
      var schema = Schema.parse(new File((n \ "@file").text))
      //val m = Map("id" -> (n \ "@id").text, "schema" -> schema)
      var schema_id = (n \ "@id").text.toInt
      scores += (schema_id -> schema)
      //      schemas_array += m
    }
    scores

  }
}
class MessageList {

  def get(file: String) = {
    var file_ = file
    if (file_.length == 0) file_ = getClass.getResource("/producer_msg.xml").getFile
    var schemas_XML = XML.loadFile(file)
    var schemas_list = (schemas_XML \\ "data" \\ "message")
    //    val schemas_array = new ArrayBuffer[Map[String, Any]]()
    var scores = new scala.collection.mutable.HashMap[String, Schema]
    var schema_id: String = null
    schemas_list.foreach { n =>
      var schema = Schema.parse(new File((n \ "@file").text))
      val m = Map("id" -> (n \ "@id").text, "schema" -> schema)
      schema_id = (n \ "@id").text
      scores += (schema_id -> schema)
      //      schemas_array += m
    }
    scores

  }

}