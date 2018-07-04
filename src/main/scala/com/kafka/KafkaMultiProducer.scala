package com.kafka

import com.core._
import com.utils._

import java.util.Properties
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

import org.json4s._
import org.json4s.native.Serialization
import org.json4s.native.Serialization.{read, write}

import java.time.Instant
import java.time.format.DateTimeFormatter
import com.github.kardapoltsev.json4s.javatime.{InstantSerializer}
import org.apache.kafka.common.serialization.StringSerializer


trait Topic[V] {
  def value: String
}

case class KafkaMultiProducer() {
  val config = new Properties()
  config.put("bootstrap.servers", "localhost:9092")
  config.put("key.serializer", classOf[StringSerializer])
  config.put("value.serializer", classOf[StringSerializer])

  val producer = new KafkaProducer[String, String](config)

  implicit val formats = Serialization.formats(NoTypeHints) +
                         InstantSerializer +
                         UriSerializer

  def send[V](value: V)(implicit topic: Topic[V]) = {
    val jsonMessage = write(value)
    val data = new ProducerRecord[String, String](topic.value, jsonMessage)

    producer.send(data)
  }

  def close() = producer.close()
}
