package com.main

import com.core._
import com.utils._
import com.kafka._
import com.cassandra._

import java.net.URI

import java.util.Properties

import org.json4s._
import org.json4s.native.Serialization
import org.json4s.native.Serialization.{read, write}

// import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord, ProducerConfig}

import java.time.Instant
import java.time.format.DateTimeFormatter
import com.github.kardapoltsev.json4s.javatime.{InstantSerializer}


object Main {
  implicit val formats = Serialization.formats(NoTypeHints) +
                         InstantSerializer +
                         UriSerializer


  def main(args: Array[String]) {
    println("------ Main ------\n")

    val uri = URI.create("http://i.prntscr.com/XXS-8L2tR7id1MSgJDywoQ.png")

    val post = Post(Id("post1"), Instant.now(), Id("user0"), "Some Text", uri, false)
    println(post)

    val user = User(Id("user0"), Instant.now(), uri, "Garfounkel", false)
    println(user)

    val comment = Comment(Id("com0"), Id("post0"), Instant.now(), Id("user0"), "Some Text", false)
    println(comment)

    // val ser = write(post)
    // val post2 = read[Post](ser)
    // println(post2)

    // val serializer = "org.apache.kafka.common.serialization.StringSerializer"
    // val config = new Properties()
    // config.put("bootstrap.servers", "localhost:9092")
    // config.put("key.serializer", serializer)
    // config.put("value.serializer", serializer)

    // val postProducer = Producer[Post](config)

    // postProducer.send(post)
    // postProducer.close()

    val postProducer = CustomKafkaProducer[Post]()
    postProducer.send(post)
    postProducer.close()

    val userProducer = CustomKafkaProducer[User]()
    userProducer.send(user)
    userProducer.close()

    val commentProducer = CustomKafkaProducer[Comment]()
    commentProducer.send(comment)
    commentProducer.close()

    // CassandraDB.createDB()

    println("\n------ Exit ------")
  }
}
