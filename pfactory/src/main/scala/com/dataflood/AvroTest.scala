package com.dataflood

import org.apache.avro.Schema
import java.io.File
import org.apache.avro.generic.GenericRecord
import org.apache.avro.generic.GenericData
import org.apache.avro.file.DataFileWriter
import org.apache.avro.generic.GenericDatumWriter
import org.apache.avro.io.DatumWriter
import org.apache.avro.Schema
import org.apache.avro.io.{ Decoder, Encoder, DatumReader, DatumWriter }
import org.apache.zookeeper.common.IOUtils
import org.apache.avro.io.EncoderFactory
import org.apache.avro.specific.SpecificDatumWriter
import org.apache.avro.io.BinaryEncoder
import java.io.ByteArrayOutputStream
import java.io.ByteArrayOutputStream
import java.io.IOException
import java.io.InputStream
import java.io.ByteArrayOutputStream
import java.io.IOException
import java.io.InputStream
import java.util.List
import java.util.Map
import java.util.Properties
//import org.apache.commons.io.IOUtils
import kafka.message.Message
import scala.xml.Node

class AvroTest {
  //  var schema = Schema.parse(new File("d:/Users/Dzmitry_Chekh/Scala_workspace/kafka_prod_cons/avro/user.avsc"))
  //println("schema ----" + schema)
  /*
  var user1 = new GenericData.Record(schema)
  user1.put("name", "Alyssa")
  user1.put("favorite_number", 256)
  // Leave favorite color null

  var user2 = new GenericData.Record(schema)
  user2.put("name", "Ben");
  user2.put("favorite_number", 7)
  user2.put("favorite_color", "red")

  var user3 = new GenericData.Record(schema)
  user3.put("name", "Jon");
  user3.put("favorite_number", 256)
  user3.put("favorite_color", "red")
*/
  def serializing (schema: Schema, n: Node) : Array[Byte] = {
    /*    var file = new File("users.avro");
    var datumWriter : DatumWriter [GenericRecord] = new GenericDatumWriter[GenericRecord](schema)
    var dataFileWriter : DataFileWriter[GenericRecord]  = new DataFileWriter[GenericRecord](datumWriter)
    dataFileWriter.create(schema, file)
    dataFileWriter.append(user1)
    dataFileWriter.append(user2)
    dataFileWriter.append(user2)
    dataFileWriter.append(user2)
    dataFileWriter.append(user2)
    dataFileWriter.append(user2)
    dataFileWriter.append(user2)
    dataFileWriter.close()
  
  *   
  */

    var user0 = new GenericData.Record(schema)
    user0.put("msg", (n \ "@msg").text)
    var stream = new ByteArrayOutputStream();
    var avroEncoderFactory = EncoderFactory.get();
    var binaryEncoder: BinaryEncoder = avroEncoderFactory.binaryEncoder(stream, null)

    var avroEventWriter = new SpecificDatumWriter[GenericRecord](schema)
    avroEventWriter.write(user0, binaryEncoder)
    //    avroEventWriter.write(user3, binaryEncoder)
    binaryEncoder.flush()
    var aa = Array[Byte]()
 //   org.apache.commons.io.IOUtils.closeQuietly(stream)
    stream.toByteArray()

  }
}