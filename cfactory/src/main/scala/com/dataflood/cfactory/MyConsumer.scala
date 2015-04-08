package com.dataflood.cfactory

import java.util.concurrent.CountDownLatch
import java.util.Properties
import kafka.consumer.{ ConsumerConfig, Whitelist, Consumer }
import scala.collection.JavaConversions._
import scala.collection.mutable.HashMap
import scala.collection.mutable.ArrayBuffer
import org.apache.avro.Schema
import org.apache.avro.generic.{ GenericRecord }
import org.apache.log4j.Logger
import java.util.Date

class MyConsumer[T](threadId: Int, cdl: CountDownLatch, cg_config: Properties, trnumGlobal: Int) extends Runnable {

  protected val logger = Logger.getLogger(getClass.getName)
  val config = new ConsumerConfig(cg_config)
  val connector = Consumer.create(config)
  val filterSpec = new Whitelist(cg_config.getProperty("topic"))
  val batch_count = cg_config.getProperty("batch_count").toInt
  val topic_type = cg_config.getProperty("topic_type").toInt
  val stream = connector.createMessageStreamsByFilter(filterSpec, 1).get(0)
  var numMessages: Int = 0
  val trnumGlobal_ = trnumGlobal
  //  val messagArray = ArrayBuffer[GenericRecord]()
  val messagArray_schemaId = ArrayBuffer[Map[Int, GenericRecord]]()
  var numMessagesTotal: Int = 0
  val p1 = new Processing
  var flushTime: Long = new Date().getTime()
  var flushFlag: Boolean = false

  def run() {
    read
    cdl.countDown()
  }

  def read = {
    //-----------    info("reading on stream now")-------------//
    //    var numMessages: Int = 0
    for (messageAndTopic <- stream) //    while (!stream.isEmpty)
    {
      try {
        //        var message = AvroWrapper.decode(messageAndTopic.message)
        var message_schemaId = AvroWrapper.decode_schemaId(messageAndTopic.message)
        var part = messageAndTopic.partition
        numMessages += 1
        numMessagesTotal += 1
        //      messagArray += message
        messagArray_schemaId += message_schemaId
        //        flushTime = new java.util.Date().getTime()
        logger.debug(("topic : " + messageAndTopic.topic + "--| " + messageAndTopic.offset.toString + s"; partition - $part , thread = $threadId , total = $numMessagesTotal"))

        flushOnTime
        /*        if (numMessages == batch_count && !flushFlag) {
          flush
          logger.debug(("topic : " + messageAndTopic.topic + "--| " + messageAndTopic.offset.toString + s"; partition - $part , thread = $threadId , total = $numMessagesTotal"))
        }
        * 
        */
      } catch {
        case e: Throwable =>
          if (true)
            println("Error processing message, skipping this message: " + e)
          else throw e
      }
    }
  }

  //---------------- close kafka connection ---------------------------------------------
  def close() {
    connector.shutdown()
  }
  //---------------- flush messages in case of batch count or time ---------------------- 
  def flushOnTime  {
    var now = new Date().getTime()
    if ((numMessages > 0 && (now - flushTime) > 5000 && !flushFlag) || numMessages == batch_count && !flushFlag ) {
      logger.info("-----thread : " + trnumGlobal_ + ", numMessages " + numMessages + ", (now - flushDate) = " + (now - flushTime))
      flush
    }
  }

  def flush {
    flushFlag = true
    p1.run(messagArray_schemaId, topic_type, trnumGlobal_)
    numMessages = 0
    messagArray_schemaId.clear()
    flushTime = new Date().getTime()
    flushFlag = false
  }

}
