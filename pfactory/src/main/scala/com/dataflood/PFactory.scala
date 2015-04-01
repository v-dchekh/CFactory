package com.dataflood

import java.util.Properties
import math._
import scala.xml.XML
import scala.collection.mutable.ArrayBuffer
import java.io.ByteArrayOutputStream
import java.io.File
import org.apache.avro.io.BinaryEncoder
import org.apache.zookeeper.common.IOUtils
import org.apache.avro.io.EncoderFactory
import org.apache.avro.generic.{ GenericDatumReader, GenericData, GenericRecord, GenericDatumWriter }
import org.apache.avro.Schema
import java.util.UUID
import org.apache.kafka.clients.producer.KafkaProducer
import kafka.producer._
import org.apache.log4j.Logger
import org.apache.log4j.BasicConfigurator

/**
 * Hello world! 
 *
 */
object PFactory {

protected val logger = Logger.getLogger(getClass.getName)
//println(getClass.getName)

  val usage = """
Usage: parser [-v] [-f file] [-s sopt] ...
Where: -v   Run verbosely
       -f F Set input file to F
       -s S Set Show option to S
"""

  var filename: String = ""
  var showme: String = ""
  var debug: Boolean = false
  val unknown = "(^-[^\\s])".r

  val pf: PartialFunction[List[String], List[String]] = {
    case "-v" :: tail =>
      debug = true; tail
    case "-f" :: (arg: String) :: tail =>
      filename = arg; tail
    case "-s" :: (arg: String) :: tail =>
      showme = arg; tail
    case unknown(bad) :: tail => endOfJob("unknown argument " + bad + "\n" + usage)
  }

  def parseArgs(args: List[String], pf: PartialFunction[List[String], List[String]]): List[String] = args match {
    case Nil => Nil
    case _   => if (pf isDefinedAt args) parseArgs(pf(args), pf) else args.head :: parseArgs(args.tail, pf)
  }

  def endOfJob(msg: String) = {
    println(msg)
    sys.exit(1)
  }

  //  lazy val producer = new KafkaProducer(writeTopic, broker)

  def main(args: Array[String]) {
    BasicConfigurator.configure()
    //logger.debug("Hello world.")
    logger.info("PFactory v0.1")
    //println("PFactory v0.1")
    // if there are required args:
    //    if (args.length == 0) die()
    val arglist = args.toList
    val remainingopts = parseArgs(arglist, pf)

    if (filename.length == 0) filename = getClass.getResource("/producer_msg.xml").getFile

    val schema_list = new SchemaList().get(filename)

    println("debug=" + debug)
    println("showme=" + showme)
    println("filename=" + filename)
    println("remainingopts=" + remainingopts)

    //"d:/Users/Dzmitry_Chekh/Scala_workspace/CFactory/bin/consumer_groups.xml"
    var prod_groups_config_XML = XML.loadFile(filename)
    var prod_groupList = (prod_groups_config_XML \\ "data" \\ "message")
    var msg: String = null
    var brk: String = null
    var topic: String = null
    var part_number: String = null
    //    var schema_id: Int = null 
    val b = new ArrayBuffer[Properties]()
    //    val kor_array = new ArrayBuffer[Map[String,Any]]()
    val kor_array = new ArrayBuffer[Any]()
    var msgc_count: Float = 0
    var avr: AvroTest = new AvroTest
    var pr2: Prod = null
    var topic_prev = ""

    prod_groupList.foreach { n =>
      msg = (n \ "@msg").text
      brk = (n \ "@brokerconnect").text
      topic = (n \ "@topic").text
      part_number = (n \ "@part_number").text
      var schema_id = (n \ "@schema_id").text.toInt

      val props_produser: Properties = new Properties
      props_produser.put("metadata.broker.list", brk)
      props_produser.put("serializer.class", "kafka.serializer.DefaultEncoder")
      props_produser.put("request.required.acks", "1")
      props_produser.put("producer.type", "sync")
      props_produser.put("queue.buffering.max.messages", "10000")
      props_produser.put("queue.buffering.max.ms", "100")
      props_produser.put("batch.num.messages", "500")

      b += props_produser
      val schema = schema_list(schema_id)
      //      println("schema_list 1 :************* " + schema)

      //println("msg : " + msg)

      if ((topic_prev != "" && topic_prev != topic)) {
        pr2.close
        //        println(" close topic_prev: " + topic_prev)
      }
      if ((topic_prev == "" || topic_prev != topic)) {
        pr2 = new Prod(props_produser, topic)
        topic_prev = topic
        //        println(" topic_prev: " + topic_prev)
      }
      //      pr2.send(avr.serializing(schema, n), part_number.getBytes(), part_number)
      val rec = new GenericData.Record(schema)
      schema_id.toInt match {
        case 0 => {
          rec.put("msg", "1")
          rec.put("name", "%null%")
        }
        case 1 => {
          rec.put("name", "2")
          rec.put("favorite_number", 1)
          rec.put("favorite_color", msg)
//          rec.put("favorite_color", "Hello \"world\" and 'city' ")
        }
        case 2 => {
          rec.put("msg", "groupName")
        }
        case 3 => {
          rec.put("action", msg)
          rec.put("name", "user5.avsc")
          rec.put("schema_body", schema.toString())
        }
      }
      //println("rec ----| " + rec)

      pr2.send(AvroWrapper.encode(rec, schema_id.toInt, schema_list), part_number.getBytes(), part_number)
      msgc_count += 1
      var d: Int = round(msgc_count / 1000)
      if ((msgc_count / 1000).toFloat == d.toFloat) {
        println(" sent messages: " + msgc_count.toInt)
      }

      //    val kor = (msg, brk, topic, 1)
      //    val m = Map("msg" -> msg, "brk" -> brk, "topic" -> topic)
      //      kor_array += m
      /*
      println(topic + ":" + part_number + ":" + msg)
      val pr = new Prod(props_produser, topic)
      pr.send(msg, part_number)
      pr.close
*/
    }
    pr2.close
    println("total sent messages: " + msgc_count.toInt)

    /*
    val schemaId = 0
    //    var schema = Schema.parse(new File("d:/Users/Dzmitry_Chekh/Scala_workspace/kafka_prod_cons/avro/user3.avsc"))

    val schemaRegistry = Map(0 -> Schema.parse(new File("d:/Users/Dzmitry_Chekh/Scala_workspace/kafka_prod_cons/avro/user3.avsc")))
    val rec = new GenericData.Record(schemaRegistry.get(schemaId).get)
    rec.put("counter", 1L)
    rec.put("name", s"ping-pong-${System.currentTimeMillis()}")
    rec.put("uuid", UUID.randomUUID().toString)

    var writeTopic: String = "len5"
    val broker = "localhost:9092,localhost:9093,localhost:9094"

    val props_produser2: Properties = new Properties
    props_produser2.put("metadata.broker.list", broker)
    props_produser2.put("serializer.class", "kafka.serializer.DefaultEncoder")
    props_produser2.put("request.required.acks", "1")
    props_produser2.put("producer.type", "sync")
    props_produser2.put("queue.buffering.max.messages", "10000")
    props_produser2.put("queue.buffering.max.ms", "100")
    props_produser2.put("batch.num.messages", "100")

    var pr3 = new Prod(props_produser2, writeTopic)
    println("********* SCLAGO ******** : " + rec)
    var part_number2 = "0"
    pr3.send(AvroWrapper.encode(rec, schemaId), part_number2.getBytes(), part_number)
    */
    //    pr3.send(AvroWrapper.encode(rec, schemaId), null)

    /*
    val props_produser: Properties = new Properties
    props_produser.put("metadata.broker.list", brk)
    //      props_produser.put("serializer.class", "kafka.serializer.StringEncoder")
    props_produser.put("serializer.class", "kafka.serializer.DefaultEncoder")

    props_produser.put("request.required.acks", "1")
    //      props_produser.put("producer.type", "async") 
    var avr: AvroTest = new AvroTest()
    //      avr.schema.toString()
    //      println(avr.schema.toString())

    val pr2 = new Prod(props_produser, topic)

    println(avr.serializing.toString())
    val par: String = "0"
    pr2.send(avr.serializing, par.getBytes("UTF8"))
    pr2.close
    * 
    */
    /*
    kor_array.foreach { n =>

      val i = n.asInstanceOf[Map[String, Any]]
      //      println(i("msg") + ":" + i("topic"))
    }
*/
    /*    b.foreach { n =>  
      println(n.getProperty("metadata.broker.list"))
    }
  
  *  
  */
    //var sch = Schema.parse(new File("user.avsc"))
    //    endOfJob("done")
    println("done")
    /*
    var groupId: String = "DimKafkaTest"
    var zookeeperConnect: String = "localhost:2181,localhost:2182,localhost:2183"
    var readFromStartOfStream = false

    val props_consumer: Properties = new Properties
    props_consumer.put("group.id", groupId)
    props_consumer.put("zookeeper.connect", zookeeperConnect)
    props_consumer.put("auto.commit.interval.ms", "1000")
    props_consumer.put("auto.offset.reset", if (readFromStartOfStream) "smallest" else "largest")
    * */

    /*
  val cons = new Cons(props_consumer, topic)
  cons.tryBatchRead(2, false)
  cons.close()

  */
    /*
  val consumer = new Cons2(topic, groupId, zookeeperConnect, true)

  var message: List[String] = List()
  //  var message: String = ""
  while (true) {
    consumer.read(bytes => {
      //        Thread.sleep(1000)
      message = bytes

      //        println("scala  > received " + message)
      //        consumer.close()
    })
    //    consumer.connector.commitOffsets(false)
    //    consumer.close
    //    println("scala  > received " + message)

 
  }
*/

    //*************** Consumer Group*****************
    /*
  var consumerGroup = new ConsumerGroupExample (5)
  consumerGroup.createConsumerConfig(zookeeperConnect, groupId)
  println(consumerGroup.createConsumerConfig(zookeeperConnect, groupId))
  consumerGroup.run

  //consumerGroup.st
*/
  }
}
