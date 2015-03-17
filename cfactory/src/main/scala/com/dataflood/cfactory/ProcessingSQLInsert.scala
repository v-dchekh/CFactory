package com.dataflood.cfactory

import scala.collection.mutable.ArrayBuffer
import org.apache.avro.generic.{ GenericRecord }
import scala.collection.mutable.{ ListMap, HashMap }

class ProcessingSQLInsert extends Processing {

  def run(messageArray: ArrayBuffer[GenericRecord]) {
    val count_ = messageArray.size
    val toSQL = ArrayBuffer[Map[String, String]]()
    val toSQLAny = ArrayBuffer[Any]()
    messageArray.foreach { x =>
      var schemaFields = x.getSchema.getFields
      var schemaDoc = x.getSchema.getDoc
      //var a = 0
      var recordToMap = new HashMap[String, Any]
      for (a <- 0 until schemaFields.size()) {
        val field = schemaFields.get(a)
        val fieldName_ = field.name()
        val fieldOrder = field.order()
        val fieldType_ = field.schema().getType
        val fieldType2_ = field.schema().getType.getName
        val fieldValue = x.get(a).toString()
        val fieldValueSQL = fieldType_.toString()  match {
          case "STRING" => {
            if (fieldValue == "%null%") "null"
            else "'" + fieldValue.replace("'", "'''") + "'"
          }
          case _ => fieldValue
        }
        //println(s"a = $a , " + fieldName_ + " = " + fieldValue + s", fieldOrder = $fieldOrder, field -> $field, fieldType -> $fieldType2_ ")
        recordToMap += (fieldName_ -> fieldValueSQL)

      }
      /*
      val recordToMapSorted = recordToMap.toSeq.sortWith(_._1 < _._1)
      val recordToMapSorted2 = recordToMap.toSeq.sortWith(_._1 > _._1)
      println("MAP    ------: " + recordToMap)
      println("MAP S  ------: " + recordToMapSorted.toMap)
      println("MAP S2 ------: " + recordToMapSorted2.toMap)
      println("MAP ls ------: " + recordToMapSorted2.toMap.values.toList.mkString("('", "','", "')"))
      println("MAP keys-----: " + recordToMapSorted2.toMap.keys.toList.mkString("insert into (", ",", ") values "))
      * 
      */
      val recordToMapSorted2 = recordToMap.toSeq.sortWith(_._1 < _._1)
      val sqlFields = recordToMapSorted2.toMap.keys.toList.mkString("(", ",", ")")
      val sqlValues = recordToMapSorted2.toMap.values.toList.mkString("(", ",", ")")
      val sqlStr = sqlFields + sqlValues
      toSQL += Map(sqlFields -> sqlValues)
      val l = (sqlFields, sqlValues, schemaDoc)
      toSQLAny += l
    }
    logger.debug("-----------------------------------------------------------------------------------------")
    logger.debug(toSQLAny.toList.mkString("\n").replace("),", ")|"))

  }

}