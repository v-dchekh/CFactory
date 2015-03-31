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
  val messagArray = ArrayBuffer[GenericRecord]()
  val messagArray_schemaId = ArrayBuffer[Map[Int, GenericRecord]]()
  val p1 = new Processing

  def run() {

    while (true) {
      //      read(messageArray => p1.run(messageArray, topic_type, threadId))
      read
      //connector.commitOffsets
    }
    cdl.countDown()
  }

  def flush {
    p1.run(messagArray_schemaId, topic_type, trnumGlobal_)
    numMessages = 0
    messagArray.clear()
    messagArray_schemaId.clear()
  }

  def read = {
    //-----------    info("reading on stream now")-------------//
    //    var numMessages: Int = 0
    var numMessagesTotal: Int = 0
    for (messageAndTopic <- stream) //    while (!stream.isEmpty)
    {
      try {
        var message = AvroWrapper.decode(messageAndTopic.message)
        var message_schemaId = AvroWrapper.decode_schemaId(messageAndTopic.message)
        var part = messageAndTopic.partition
        numMessages += 1
        numMessagesTotal += 1
        messagArray += message
        messagArray_schemaId += message_schemaId
        logger.debug(("topic : " + messageAndTopic.topic + "--| " + messageAndTopic.offset.toString + s"; partition - $part , thread = $threadId , total = $numMessagesTotal"))

        if (numMessages == batch_count) {
          flush

          /*          numMessages = 0
          messagArray.clear()
          messagArray_schemaId.clear()
          * 
          */
          logger.info(("topic : " + messageAndTopic.topic + "--| " + messageAndTopic.offset.toString + s"; partition - $part , thread = $threadId , total = $numMessagesTotal"))
        }
      } catch {
        case e: Throwable =>
          if (true)
            println("Error processing message, skipping this message: " + e)
          else throw e
      }
    }
  }

  def close() {
    connector.shutdown()
  }
}
