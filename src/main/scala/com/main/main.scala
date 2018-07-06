package com.main

import com.core._
import com.utils._
import com.kafka._
import com.cassandra._

import java.net.URI

import java.util.Properties

import util.control.Breaks._

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

    val post = Post(Id("post6"), Instant.now(), Id("Garfounkel"), "Some Text", uri, false)
    val user = User(Id("Garfounkel"), Instant.now(), uri, false)
    val comment = Comment(Id("com6"), Id("post0"), Instant.now(), Id("Garfounkel"), "Some Text", false)

    // val postProducer = KafkaMultiProducer()
    // postProducer.send(post)
    // postProducer.close()
    //
    // val userProducer = KafkaMultiProducer()
    // userProducer.send(user)
    // userProducer.close()
    //
    // val commentProducer = KafkaMultiProducer()
    // commentProducer.send(comment)
    // commentProducer.close()

    val groupId = "group"
    val brokers = "localhost:9092"

    val consumer_users = new ConsumerExecutor(brokers, groupId + 1, "users")
    consumer_users.run()

    val consumer_messages = new ConsumerExecutor(brokers, groupId + 2, "messages")
    consumer_messages.run()

    val consumer_posts = new ConsumerExecutor(brokers, groupId + 3, "posts")
    consumer_posts.run()

    breakable {
      while (true) {
        println("Enter the operation you need (query/produce)")
        print("> ")
        val input = scala.io.StdIn.readLine()
        if (input == "query") {
          println("What word are you looking for? ")
          print("> ")
          // get input for request again?
        }
        else if (input == "produce") {

        }
        else if (input == "exit") {
          break
        }
        else {
          println("Unknown operation")
        }
      }
    }

    println("\n------ Exit ------")
  }
}
