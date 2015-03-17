package com.dataflood

import java.io.ByteArrayOutputStream
import java.nio.ByteBuffer
import java.util.UUID
import kafka.consumer._
import kafka.producer._
import org.apache.avro.Schema
import org.apache.avro.generic.{ GenericDatumReader, GenericData, GenericRecord, GenericDatumWriter }
import org.apache.avro.io.{ DecoderFactory, EncoderFactory }
import java.io.File
import scala.collection.mutable.HashMap

object AvroWrapper  {
  final val MAGIC = Array[Byte](0x0)

  val schemaRegistry = Map(0 -> Schema.parse(new File("d:/Users/Dzmitry_Chekh/Scala_workspace/kafka_prod_cons/avro/user3.avsc")))

  def encode(obj: GenericRecord, schemaId: Int, schema_list :  HashMap[Int, Schema]): Array[Byte] = {
    val out = new ByteArrayOutputStream()
    out.write(MAGIC)
    out.write(ByteBuffer.allocate(4).putInt(schemaId).array())

    val encoder = EncoderFactory.get().directBinaryEncoder(out, null)
    val schemaOpt = schema_list.get(schemaId)
    if (schemaOpt.isEmpty) throw new IllegalArgumentException("Invalid schema id")

    val writer = new GenericDatumWriter[GenericRecord](schemaOpt.get)
    writer.write(obj, encoder)

    out.toByteArray
  }

  def decode(bytes: Array[Byte]): GenericRecord = {
    val decoder = DecoderFactory.get().binaryDecoder(bytes, null)
    val magic = new Array[Byte](1)
    decoder.readFixed(magic)
    if (magic.deep != MAGIC.deep) throw new IllegalArgumentException("Not a camus byte array")

    val schemaIdArray = new Array[Byte](4)
    decoder.readFixed(schemaIdArray)

    val schemaOpt = schemaRegistry.get(ByteBuffer.wrap(schemaIdArray).getInt)
    schemaOpt match {
      case None => throw new IllegalArgumentException("Invalid schema id")
      case Some(schema) =>
        val reader = new GenericDatumReader[GenericRecord](schema)
        reader.read(null, decoder)
    }
  }
}