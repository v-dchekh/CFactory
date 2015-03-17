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

class MyConsumer[T](mes: String, cdl: CountDownLatch, cg_config: Properties) extends Runnable {

  protected val logger = Logger.getLogger(getClass.getName)

  val config = new ConsumerConfig(cg_config)
  val connector = Consumer.create(config)
  val filterSpec = new Whitelist(cg_config.getProperty("topic"))
  val batch_count = cg_config.getProperty("batch_count").toInt
  val topic_type = cg_config.getProperty("topic_type").toInt

  val stream = connector.createMessageStreamsByFilter(filterSpec, 1).get(0)

  def run() {
    val p1 = new Processing
    while (true) read(messageArray => p1.run(messageArray, topic_type))
    cdl.countDown()
  }

  def read(processing: ArrayBuffer[GenericRecord] => Unit) = {
    //-----------    info("reading on stream now")-------------//
    var numMessages: Int = 0
    var numMessagesTotal: Int = 0
    val messagArray = ArrayBuffer[GenericRecord]()
    for (messageAndTopic <- stream) //    while (!stream.isEmpty)
    {
      try {
        var message = AvroWrapper.decode(messageAndTopic.message)
        var part = messageAndTopic.partition
        numMessages += 1
        numMessagesTotal += 1
        messagArray += message
        logger.debug(("topic : " + cg_config.getProperty("topic") + "--| " + messageAndTopic.offset.toString + s"; partition - $part , thread = $mes , total = $numMessagesTotal"))

        if (numMessages == batch_count) {
          processing(messagArray)
          numMessages = 0
          messagArray.clear()
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
