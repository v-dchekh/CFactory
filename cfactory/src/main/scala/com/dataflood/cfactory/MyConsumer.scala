package com.dataflood.cfactory

import java.util.concurrent.CountDownLatch
import java.util.Properties
import kafka.consumer.{ ConsumerConfig, Whitelist, Consumer }
import scala.collection.JavaConversions._
import scala.collection.mutable.HashMap
import scala.collection.mutable.ArrayBuffer
import org.apache.avro.Schema
import org.apache.avro.generic.GenericRecord
import org.apache.log4j.Logger
import java.util.Date

class MyConsumer[T](threadId: Int, cdl: CountDownLatch, cgConfig: Properties, trnumGlobal: Int) extends Runnable {

  protected val logger = Logger.getLogger(getClass.getName)
  private val connector = Consumer.create(new ConsumerConfig(cgConfig))
  private val batchCount = cgConfig.getProperty("batch_count").toInt
  private val topicType = cgConfig.getProperty("topic_type").toInt
  private val filterSpec = new Whitelist(cgConfig.getProperty("topic"))
  private val stream = connector.createMessageStreamsByFilter(filterSpec, 1).get(0)
  private var numMessages: Int = 0
  var numMessagesTotal: Int = 0
  val trnumGlobal_ = trnumGlobal
  //  val messagArray = ArrayBuffer[GenericRecord]()
  private val messagArraySchemaId = ArrayBuffer[Map[Int, GenericRecord]]()
  private val p1 = new Processing
  private var flushTime: Long = new Date().getTime()
  private var flushFlag: Boolean = false

  def run() {
    //-----------    info("reading on stream now")-------------//
    //    var numMessages: Int = 0
    for (messageAndTopic <- stream) //    while (!stream.isEmpty)
    {
      try {
        //        var message = AvroWrapper.decode(messageAndTopic.message)
        val message_schemaId = AvroWrapper.decodeSchemaId(messageAndTopic.message)
        val part = messageAndTopic.partition
        numMessages += 1
        numMessagesTotal += 1
        //      messagArray += message
        messagArraySchemaId += message_schemaId
        logger.debug(("topic : " + messageAndTopic.topic + "--| " + messageAndTopic.offset.toString + s"; partition - $part , thread = $threadId , total = $numMessagesTotal"))

        flushOnTime
      } catch {
        case e: Throwable =>
          if (true)
            println("Error processing message, skipping this message: " + e)
          else throw e
      }
    }
    cdl.countDown()
  }

  //---------------- close kafka connection ---------------------------------------------
  def close() {
    connector.shutdown()
  }
  //---------------- flush messages in case of batch count or time ---------------------- 
  def flushOnTime {
    if (numMessages == batchCount && !flushFlag) {
      var now = new Date().getTime()
      logger.info("-----thread : " + trnumGlobal_ + ", numMessages " + numMessages + ", (now - flushDate) = " + (now - flushTime))
      flush
    } else {
      if ((numMessages > 0 && !flushFlag)) {
        var now = new Date().getTime()
        if ((now - flushTime) > 5000) {
          logger.info("-----thread : " + trnumGlobal_ + ", numMessages " + numMessages + ", (now - flushDate) = " + (now - flushTime))
          flush
        }
      }
    }
  }

  def flush {
    flushFlag = true
    p1.run(messagArraySchemaId, topicType, trnumGlobal_)
    numMessages = 0
    messagArraySchemaId.clear()
    flushTime = new Date().getTime()
    flushFlag = false
  }

}
