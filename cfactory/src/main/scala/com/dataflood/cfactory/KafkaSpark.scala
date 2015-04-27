package com.dataflood.cfactory

import java.util.Properties

import kafka.producer._

import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka._
import org.apache.spark.SparkConf



class KafkaSpark {
   println ("---------------KafkaSpark-------------")
   //val kafkaStream = KafkaUtils.createStream(ssc, zkQuorum, groupId, topics, storageLevel)
   
   val _zkQuorum = "localhost:2181,localhost:2182,localhost:2183" 
   val _group = "spark"
   val _topics = "dbo_group2_CT"
   val Array(zkQuorum, group, topics, numThreads) = Array(_zkQuorum,_group, _topics , "2")
    val sparkConf = new SparkConf().setAppName("KafkaWordCount")
    val ssc =  new StreamingContext(sparkConf, Seconds(2))
    ssc.checkpoint("checkpoint")

    val topicMap = topics.split(",").map((_,numThreads.toInt)).toMap
    val lines = KafkaUtils.createStream(ssc, zkQuorum, group, topicMap).map(_._2)
    val words = lines.flatMap(_.split(" "))
    val wordCounts = words.map(x => (x, 1L))
      .reduceByKeyAndWindow(_ + _, _ - _, Minutes(10), Seconds(2), 2)
    wordCounts.print()

    ssc.start()
   
   //val ssc = new StreamingContext(sparkConf, Seconds(2))
   
/*   
   val ssc: StreamingContext = ???
  val kafkaParams: Map[String, String] = Map("group.id" -> "terran", ...)
  val readParallelism = 5
  val topics = Map("zerg.hydra" -> 1)

val kafkaDStreams = (1 to readParallelism).map { _ =>
    KafkaUtils.createStream(ssc, kafkaParams, topics, ...)
  }
   
   val kafkaStream = KafkaUtils.createStream(streamingContext, [zookeeperQuorum], [group id of the consumer], [per-topic number of Kafka partitions to consume])*/
}