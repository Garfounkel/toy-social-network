package com.kafka

import com.utils._

import scala.collection.JavaConverters._
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.common.serialization.StringDeserializer
import org.json4s.ParserUtil._
import java.util.Properties
import java.util.Collections._

import org.json4s._
import org.json4s.native.Serialization
import org.json4s.native.Serialization.{read, write}

import java.time.Instant
import java.time.format.DateTimeFormatter
import com.github.kardapoltsev.json4s.javatime.{InstantSerializer}


case class CustomKafkaConsumer[V](implicit topic: Topic[V]) {
  val properties = new Properties()
  properties.put("bootstrap.servers", "localhost:9092")
  properties.put("group.id", "some-group")
  properties.put("key.deserializer", classOf[StringDeserializer])
  properties.put("value.deserializer", classOf[StringDeserializer])

  val consumer = new KafkaConsumer[String, String](properties)
  consumer.subscribe(singletonList(topic.value))

  implicit val formats = Serialization.formats(NoTypeHints) +
                         InstantSerializer +
                         UriSerializer

  def readFromBegining[V]()(implicit topic: Topic[V], m: Manifest[V]): Iterable[V] = {
    consumer.poll(0)
    consumer.seekToBeginning(consumer.assignment())

    readStream[V]()
  }

  def readStream[V]()(implicit topic: Topic[V], m: Manifest[V]): Iterable[V] = {
    val results = consumer.poll(2000).asScala
    results.map(x => read[V](x.value))
      //while(True) {
      // for (record <- results) {
      //   try {
      //     val obj = read[V](record.value)
      //     println(obj)
      //   } catch {
      //     case e: ParseException => println(e)
      //   }
      // }
      // }
  }

  def close() = consumer.close()
}
