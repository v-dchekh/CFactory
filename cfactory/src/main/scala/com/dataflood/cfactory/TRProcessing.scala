package com.dataflood.cfactory

import scala.collection.mutable.ArrayBuffer
import org.apache.avro.generic.{ GenericRecord }
import scala.collection.mutable.HashMap
import scala.xml.XML
import java.nio.file.{ Paths, Files }
import java.nio.charset.StandardCharsets

import org.apache.log4j.Logger

trait TRProcessing {
  def run(x: ArrayBuffer[GenericRecord], topic_type: Int)
}

class Processing extends TRProcessing {
  protected val logger = Logger.getLogger(getClass.getName)

  var topic_type_ : Int = 0
  def run(messageArray: ArrayBuffer[GenericRecord], topic_type: Int) {
    topic_type_ = topic_type
    topic_type match {
      case 1 =>
        val pr = new ProcessingSystem().run(messageArray)
      case _ =>
        val pr = new ProcessingSQLInsert().run(messageArray)
    }
  }
}

class ProcessingSystem extends Processing {

  private def createElement(atr1: Option[String], atr2: Option[String]) = {
    <schema id={ atr1.getOrElse(null) } file={ atr2.getOrElse(null) }/>
  }
  private def schemasListRefresh {
    logger.debug("topic_type = refresh")
    CFactory.schema_list = Configurations.getSchemaList(XML.loadFile(CFactory.filename))
  }
  private def schemasListAdd(fieldSchemaValue: String) {
    logger.debug("topic_type = add")
    val path = "d:/Users/Dzmitry_Chekh/Avro_schemas/user5.avsc"
    Files.write(Paths.get(path), fieldSchemaValue.getBytes(StandardCharsets.UTF_8))
    println(createElement(Some("5"), Some(path)))
    schemasListRefresh
  }
  private def schemasListDelete {
    logger.debug("topic_type = delete")
    schemasListRefresh
  }

  def run(messageArray: ArrayBuffer[GenericRecord]) {
    messageArray.foreach { x =>
      val schemaFields = x.getSchema.getFields
      val schemaDoc = x.getSchema.getDoc
      val recordToMap = HashMap[String, Any]()
      //------------- get an Action field (name and value) ----------------)      
      val fieldAction = schemaFields.get(0)
      val fieldActionName = fieldAction.name()
      val fieldActionValue = x.get(0).toString()

      //------------- get a Schema Body field (name and value) ------------)      
      val fieldAvroSchema = schemaFields.get(1)
      val fieldAvroSchemaName = fieldAction.name()
      val fieldAvroSchemaValue = x.get(1).toString()

      fieldActionName match {
        case "action" => {
          fieldActionValue match {
            case "add"     => schemasListAdd(fieldAvroSchemaValue)
            case "delete"  => schemasListDelete
            case "refresh" => schemasListRefresh
            case _         => println(s"topic_type is not in (refresh)")
          }
          //println(CFactory.schema_list.mkString("\n"))
        }
      }
    }
  }
}
