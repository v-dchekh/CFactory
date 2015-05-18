package com.dataflood.pfactorymssql

import java.util.concurrent.CountDownLatch
import org.apache.log4j.Logger
import scala.collection.mutable.ArrayBuffer
import org.apache.avro.generic.GenericRecord
import kafka.producer.ProducerConfig
import kafka.producer.Producer
import java.util.Properties
import kafka.producer.KeyedMessage
import scala.collection.mutable.HashMap

class KfProducer(topicName: String, a: Int, cdl: CountDownLatch, finish: (KfProducer) => Unit) extends Runnable {
  protected val logger = Logger.getLogger(getClass.getName)
  var trNum = a
  var topic = topicName
  var props: Properties = new Properties
  //val config = new ProducerConfig(props)
  //val producer = new Producer[AnyRef, AnyRef](config)
  var rowNumber: Int = -1
  override def run() {
    try {

      val sqlData = getData
      //logger.debug("\n" + sqlData.toList.mkString("\n"))
      //sendToKafka(sqlData)
      launch
    } finally {
    }
  }
  /*
  def kafkaMesssage(message: Array[Byte], key: Array[Byte], partition: String): KeyedMessage[AnyRef, AnyRef] = {
    if (partition == null) {
      new KeyedMessage(topic, message)
    } else {
      new KeyedMessage(topic, key, partition, message)
    }
  }

  def send(message: Array[Byte], key: Array[Byte], partition: String): Unit = {
    try {
      producer.send(kafkaMesssage(message, key, partition))
    } catch {
      case e: Exception =>
        e.printStackTrace
        System.exit(1)
    }
  }
*/
  def getData = {
    val connect = App.poolSQLConnect.borrowObject()
    var arrayRows = new ArrayBuffer[HashMap[String, String]]()
    try {
      try {
        rowNumber = 0
        val pstmt = connect.prepareCall("{call dbo.cdcGetData(?)}")
        pstmt.setNString(1, "dbo_user_")
        //pstmt.registerOutParameter(2, java.sql.Types.INTEGER)
        val rs = pstmt.executeQuery()
        val rsmd = rs.getMetaData()
        val cols = rsmd.getColumnCount()
        while (rs.next()) {
          val arrayRow = new HashMap[String, String]
          for (i <- 1 to cols) arrayRow += (rsmd.getColumnName(i) -> rs.getString(i))
          arrayRows += arrayRow
        }
        rowNumber = arrayRows.size
        logger.info("rowNumber - " + trNum + " - " + rowNumber)

        //logger.debug("\n" + arrayRows.toList.mkString("\n"))

        pstmt.close
        //        logger.debug("\n" + arrayRows.toList.mkString("\n"))

      } catch {
        case e: Throwable =>
          if (true) {
            logger.error(" trNum " + trNum + "; rowNumber " + rowNumber +"  ---" +e)
            rowNumber = -1
          }
          else {
            logger.error(" trNum " + trNum + "; rowNumber " + rowNumber +"  ---" +e)
            throw e
          }
      }
    } finally {
      App.poolSQLConnect.returnObject(connect)
    }
    //logger.debug("\n" + arrayRows.toList.mkString("\n"))
    arrayRows
  }

  def sendToKafka(data: ArrayBuffer[HashMap[String, String]]) {
    data.foreach {
      x =>
        val recordArray = x.toArray
        val schema_Id = recordArray(0)._1
        val genRecord = recordArray(0)._2
        //        val message = AvroWrapper.encode(genRecord, schema_Id)
        val part_number: String = x.hashCode().toString()
        logger.info(schema_Id + " " + genRecord)
      //      send(message, part_number.getBytes(), part_number)
    }
  }

  def setParams(topicName: String, a: Int) {
    trNum = a
    topic = topicName
    logger.info(topic + " ------------ " + trNum)
  }

  def launch {
    val r = scala.util.Random
    //val randomInt = 1 //r.nextInt(10000)
    //logger.info(topic + " start  " + " #" + trNum + " " + randomInt)
    //Thread.sleep(randomInt)
    //logger.info(topic + " finish " + " #" + trNum + " " + randomInt)
    cdl.countDown()
    finish(this)

  }

}
