package com.kafka

import com.core._
import com.utils._

import java.util.Properties
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord, ProducerConfig}

import org.json4s._
import org.json4s.native.Serialization
import org.json4s.native.Serialization.{read, write}

import java.time.Instant
import java.time.format.DateTimeFormatter
import com.github.kardapoltsev.json4s.javatime.{InstantSerializer}


object PostProducer {
    implicit val formats = Serialization.formats(NoTypeHints) +
                           InstantSerializer +
                           UriSerializer

  def testProducer(post: Post) = {
    val serializer = "org.apache.kafka.common.serialization.StringSerializer"
    val config = new Properties()
    config.put("bootstrap.servers", "localhost:9092")
    config.put("key.serializer", serializer)
    config.put("value.serializer", serializer)

    val producer = new KafkaProducer[String, String](config)
    val jsonMessage = write(post)
    val data = new ProducerRecord[String, String]("post", jsonMessage)

    producer.send(data)
    producer.close()
  }
}
