package com.main

import com.core._
import com.utils._
import com.kafka._
import com.query._
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
import org.apache.kafka.common.errors.WakeupException


object Main {
  implicit val formats = Serialization.formats(NoTypeHints) +
                         InstantSerializer +
                         UriSerializer


  def main(args: Array[String]) {
    /*
    val uri = URI.create("http://i.prntscr.com/XXS-8L2tR7id1MSgJDywoQ.png")

    val post = Post(Id("post6"), Instant.now(), Id("Garfounkel"), "Some Text", uri, false)
    val user = User(Id("Garfounkel"), Instant.now(), uri, false)
    val comment = Comment(Id("com6"), Id("post0"), Instant.now(), Id("Garfounkel"), "Some Text", false)*/
    val msg = Message(Id("msg01"), Instant.now(), Id("Garfounkel"), Id("Toto"), "Some message", false)

    println(write(msg))

    println("------ Main ------\n")
    // consumer goes here
    if (args.size > 0 && args(0) == "listener") {
      val main = Thread.currentThread()
      Listen()
      main.interrupt()
    }
    else { // if its not a consumer, then start the shell
      InteractiveQuery()
    }
    println("\n------ Exit ------")
  }


  def Listen() = {
    // CassandraDB
    // CassandraDB.createDB()

    // Consumers
    val groupId = "group"
    val brokers = "localhost:9092"

    val consumer_users = new ConsumerExecutor[User](brokers, groupId + 1)
    val consumer_msgs = new ConsumerExecutor[Message](brokers, groupId + 2)
    val consumer_posts = new ConsumerExecutor[Post](brokers, groupId + 3)

    val consumers = List(consumer_users, consumer_msgs, consumer_posts)
    consumers.foreach(x => x.run())

    // Safely exit consumers
    breakable {
      println()
      while (true) {
        println("Listening on multiple topics: users, posts and messages...")
        println("Enter exit to safely shutdown all threads and consumers.")
        print("> ")
        val input = scala.io.StdIn.readLine()
        if (input == "exit") {
          consumers.foreach(x => x.shutdown())
          break
        }
        else {
          println("Unknown operation\n")
        }
      }
    }
  }

  def InteractiveQuery() = {
    breakable {
      while (true) {
        println("Enter the operation you need (query/produce/cachetohdfs)")
        print("> ")
        val input = scala.io.StdIn.readLine()
        if (input == "query") {
          println("What word are you looking for? ")
          print("> ")
          val input = scala.io.StdIn.readLine()
          if (input == "query") {
            println("What word are you looking for? ")
            print("> ")
            val query = scala.io.StdIn.readLine()
            val tmp = Query.LoadMessages()
            tmp.foreach(x => println(x))
          }
          else if (input == "produce") {

          }
          else if (input == "cachetohdfs") {
            CassandraDB.toHDFS()
          }
          else if (input == "exit") {
            break
          }
          else {
            println("Unknown operation")
          }
        }
      }
    }
  }
}
