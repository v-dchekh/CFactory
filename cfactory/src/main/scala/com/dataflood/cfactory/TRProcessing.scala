package com.dataflood.cfactory

import scala.collection.mutable.{ ArrayBuffer, HashMap }
import scala.xml.{ XML, Elem, Node }
import org.apache.avro.generic.{ GenericRecord }
import org.apache.log4j.Logger
import java.nio.file.{ Paths, Files }
import java.nio.charset.StandardCharsets
import com.google.gson.{ JsonParser, GsonBuilder }

trait TRProcessing {
  def run(x: ArrayBuffer[Map[Int,GenericRecord]], topic_type: Int , threadId : Int)
}

class Processing extends TRProcessing {
  protected val logger = Logger.getLogger(getClass.getName)

  var topic_type_ : Int = 0
  def run(messageArray: ArrayBuffer[Map[Int,GenericRecord]], topic_type: Int, threadId : Int) {
    topic_type_ = topic_type
    topic_type match {
      case 1 =>
        val pr = new ProcessingSystem().run(messageArray)
      case _ =>
        val pr = new ProcessingSQLInsert().run(messageArray, threadId)
    }
  }
}

class ProcessingSystem extends Processing {

  def run(messageArray: ArrayBuffer[Map[Int,GenericRecord]]) {
    var fieldActionValue = new (String)
    var fieldAvroNameValue = new (String)
    var fieldAvroSchemaValue = new (String)

    def createElement(atr1: Option[String], atr2: Option[String]) = {
      <schema file={ atr1.getOrElse(null) } id={ atr2.getOrElse(null) }/>
    }

    //------------- JSON Pretty Print ----------------)      
    def jsonPrettyPrint(input: String) = {
      var gson = new GsonBuilder().setPrettyPrinting().create()
      var jp = new JsonParser
      var prettyJsonString = gson.toJson(jp.parse(input))
      prettyJsonString
    }

    def schemasListRefresh = CFactory.schemaList = Configurations.getSchemaList()

    //------------- Added new schema ----------------)      
    def schemasListAdd {
      def addChild(n: Node, newChild: Node) = n match {
        case Elem(prefix, label, attribs, scope, child @ _*) =>
          Elem(prefix, label, attribs, scope, child ++ newChild: _*)
        case _ => error("Can only add children to elements!")
      }

      //------------- Write schema to a file ----------------)      
      val path = Configurations.getSchemaPath() + fieldAvroNameValue
      val schema_json = jsonPrettyPrint(fieldAvroSchemaValue)
      Files.write(Paths.get(path), schema_json.getBytes(StandardCharsets.UTF_8))

      //------------- Adde new schema record to the config file ----------------)      
      var schemaList_XML = (CFactory.cfgXML \\ "schemas")
      var consumer_groups_XML = (CFactory.cfgXML \\ "consumer_groups")
      var consumer_config_XML = (CFactory.cfgXML \\ "consumer_config")

      var newSchema = createElement(Some(fieldAvroNameValue), Some("5"))
      var root: Node = schemaList_XML(0)
      schemaList_XML = addChild(root, newSchema)
      schemaList_XML = <body>{ consumer_config_XML }{ consumer_groups_XML }{ schemaList_XML }</body>

      val prettyPrinter = new xml.PrettyPrinter(180, 4)
      val prettyXml = prettyPrinter.formatNodes(schemaList_XML)
      XML.save(CFactory.filename, XML.loadString(prettyXml), "UTF-8", true, null)
      //      logger.debug(prettyXml)
      schemasListRefresh
    }

    def schemasListDelete {
      schemasListRefresh
    }
    messageArray.foreach { x =>
      val recordArray  = x.toArray
      val record = recordArray(0)._2 
      val schemaFields = record.getSchema.getFields
      val schemaDoc = record.getSchema.getDoc
      val recordToMap = HashMap[String, Any]()
      //------------- get an Action field (name and value) ----------------)      
      val fieldAction = schemaFields.get(0)
      val fieldActionName = fieldAction.name()
      fieldActionValue = record.get(0).toString()

      //------------- get a Schema Name field (name and value) ------------)      
      val fieldAvroName = schemaFields.get(1)
      val fieldAvroNameName = fieldAction.name()
      fieldAvroNameValue = record.get(1).toString()

      //------------- get a Schema Body field (name and value) ------------)      
      val fieldAvroSchema = schemaFields.get(2)
      val fieldAvroSchemaName = fieldAction.name()
      fieldAvroSchemaValue = record.get(2).toString()

      logger.debug("---------------------------------------------------------------")
      logger.debug("action_type = " + fieldActionValue + "," + fieldAvroNameValue) //+ "," + fieldAvroSchemaValue)
      logger.debug("---------------------------------------------------------------")

      fieldActionName match {
        case "action" => {
          fieldActionValue match {
            case "add"     => schemasListAdd
            case "delete"  => schemasListDelete
            case "refresh" => schemasListRefresh
            case _         => logger.error("action is not in (add,delete,refresh) = "+ fieldActionValue) 
          }
          //println(CFactory.schemaList.mkString("\n"))
        }
      }
    }
  }
}
