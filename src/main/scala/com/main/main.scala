package com.main

import com.core._
import com.utils._
import com.kafka._
import com.cassandra._

import java.net.URI

import java.util.Properties
import java.util.Collections._

import org.json4s._
import org.json4s.native.Serialization
import org.json4s.native.Serialization.{read, write}

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

    val post = Post(Id("post6"), Instant.now(), Id("Garfounkel"), "Some Text", uri, false)
    val user = User(Id("Garfounkel"), Instant.now(), uri, false)
    val comment = Comment(Id("com6"), Id("post0"), Instant.now(), Id("Garfounkel"), "Some Text", false)

    val producer = KafkaMultiProducer()
    producer.send(user)
    producer.close()

    val consumer = CustomKafkaConsumer[User]()
    val usersFromTopic = consumer.readFromBegining[User]()
    consumer.close()

    println(usersFromTopic)

    // CassandraDB.createDB()

    println("\n------ Exit ------")
  }
}
