package com.dataflood.pfactorymssql

import org.apache.commons.pool2.impl.DefaultPooledObject
import org.apache.commons.pool2.{ PooledObject, BasePooledObjectFactory }
import java.util.concurrent.CountDownLatch

/**
 * An object factory for Kafka producer apps, which is used to create a pool of such producers (think: DB connection
 * pool).
 *
 * We use this class in our Spark Streaming examples when writing data to Kafka.  A pool is typically the preferred
 * pattern to minimize TCP connection overhead when talking to Kafka from a Spark cluster.  Another reason is to to
 * reduce the number of TCP connections being established with the cluster in order not to strain the cluster.
 *
 * See the Spark Streaming Programming Guide, section "Design Patterns for using foreachRDD" in
 * [[http://spark.apache.org/docs/1.1.0/streaming-programming-guide.html#output-operations-on-dstreams Output Operations on DStreams]]
 */
// TODO: Time out / shutdown producers if they haven't been used in a while.

abstract class KafkaProducerAppFactory2(topic: String, a: Int)
  extends Serializable {
  def newInstance(): KfProducer
}

class BaseKafkaProducerAppFactory2(topic: String, a: Int, latch: CountDownLatch, finish: (KfProducer) => Unit)
  extends KafkaProducerAppFactory2(topic, a) {
  override def newInstance() = new KfProducer(topic, a, latch, finish)
}

class PooledKafkaProducerAppFactory2(val factory: KafkaProducerAppFactory2)
  extends BasePooledObjectFactory[KfProducer] with Serializable {

  override def create(): KfProducer = factory.newInstance()

  override def wrap(obj: KfProducer): PooledObject[KfProducer] = new DefaultPooledObject(obj)

  // From the Commons Pool docs: "Invoked on every instance when it is being "dropped" from the pool.  There is no
  // guarantee that the instance being destroyed will be considered active, passive or in a generally consistent state."
  override def destroyObject(p: PooledObject[KfProducer]): Unit = {
    super.destroyObject(p)
  }

}