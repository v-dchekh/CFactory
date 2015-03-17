package com.dataflood

import kafka.producer.KeyedMessage
import kafka.producer.ProducerConfig
import java.util.Properties
import kafka.producer.Producer

class Prod(props: Properties, topic: String) {
  /*  val props: Properties = new Properties
  props.put("metadata.broker.list", "localhost:9092,localhost:9093,localhost:9094")
  props.put("serializer.class", "kafka.serializer.StringEncoder")
  props.put("request.required.acks", "1")*/
  val config = new ProducerConfig(props)
  val producer = new Producer[String, String](config)
  val producer2 = new Producer[AnyRef, AnyRef](config)
  //  val producer = new Producer[AnyRef, AnyRef](new ProducerConfig(props))
  /*
  def sendMessages(msg: String) {
    val data = new KeyedMessage[String, String](topic, msg)
    producer.send(data)
  }
  
  */

  def kafkaMesssage(message: String, partition: String): KeyedMessage[String, String] = {
    if (partition == null) {
      new KeyedMessage(topic, message)
    } else {
      new KeyedMessage(topic, partition, message)
    }
  }

  def kafkaMesssage2(message: Array[Byte], key: Array[Byte] , partition : String): KeyedMessage[AnyRef, AnyRef] = {
    if (partition == null) {
      new KeyedMessage(topic, message)
    } else {
      new KeyedMessage(topic, key, partition, message)
    }
  }

  //  def send(message: String, partition: String = null): Unit = send(message.getBytes("UTF8"), if (partition == null) null else partition.getBytes("UTF8"))

  def send(message: String, partition: String): Unit = {
    try {
      producer.send(kafkaMesssage(message, partition))
    } catch {
      case e: Exception =>
        e.printStackTrace
        System.exit(1)
    }
  }

  def send(message: Array[Byte], key: Array[Byte], partition : String): Unit = {
    try {
      producer2.send(kafkaMesssage2(message, key, partition))
    } catch {
      case e: Exception =>
        e.printStackTrace
        System.exit(1)
    }
  }

  def Test: List[String] = {
    val m: List[String] = List("131313")
    m
  }

  def close {
    producer.close()
  }
  //        long events = Long.parseLong(args[0]);
  //  val rnd: Random = new Random
  /*
    val config: ProducerConfig = new ProducerConfig(props)
    val producer: Producer[String, String] = new Producer[String, String](config)

    for( x <- 1 to messNumber) {
      val runtime = new Date().getTime
      val ip = "192.168.2." + rnd.nextInt(255)
      val msg = runtime + ", privet ," + ip
      val data: KeyedMessage[String, String] = new KeyedMessage[String, String]("len.hydra", msg)
      producer.send(data)
      println(msg)
    }
    */
  //  println("sent: "+messNumber)

}