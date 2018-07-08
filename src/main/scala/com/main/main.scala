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
import java.time.temporal.ChronoUnit
import com.github.kardapoltsev.json4s.javatime.{InstantSerializer}
import org.apache.kafka.common.errors.WakeupException


object Main {
  implicit val formats = Serialization.formats(NoTypeHints) +
                         InstantSerializer +
                         UriSerializer


  def main(args: Array[String]) {
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
    CassandraDB.createDB()

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
        println("Enter the operation you need (query/cachetohdfs/produce/exit)")
        print("> ")
        val input = scala.io.StdIn.readLine()
        if (input == "query") {

          println("What word are you looking for?")
          print("> ")
          val query = scala.io.StdIn.readLine()

          println("Since when? (number in days)")
          print("> ")
          val days = scala.io.StdIn.readLine()

          try {
            val messages = Query.SearchMessagesSince(query, days.toInt, ChronoUnit.DAYS)
            val posts = Query.SearchPostsSince(query, days.toInt, ChronoUnit.DAYS)

            println("messages resulting the query " + query + ": ")
            messages.foreach(x => println("\tfrom: " + x.author.value + ", to: " + x.dest.value + ", message: " + x.text + ", at date: " + x.updatedOn))
            println()
            println("posts resulting the query " + query + ": ")
            posts.foreach(x => println("\tfrom: " + x.author.value + ", message: " + x.text + ", at date: " + x.updatedOn))
          }
          catch {
            case e : Exception => {
              val messages = Query.SearchMessages(query)
              val posts = Query.SearchPosts(query)

              println("messages resulting the query " + query + ": ")
              messages.foreach(x => println("from: " + x.author.value + ", to: " + x.dest.value + ", message: " + x.text + ", at date: " + x.updatedOn))
              println()
              println("posts resulting the query " + query + ": ")
              posts.foreach(x => println("from: " + x.author.value + ", message: " + x.text + ", at date: " + x.updatedOn))
            }
          }

        }
        else if (input == "produce") {
          ProduceData()
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

  def ProduceData() = {
    val uri = URI.create("http://i.prntscr.com/XXS-8L2tR7id1MSgJDywoQ.png")

    val post = Post(Id("post1"), Instant.now(), Id("user1"), "Some Text", uri, false)

    val user1 = User(Id("user1"), Instant.now(), uri, false)
    val user2 = User(Id("user2"), Instant.now(), uri, false)

    val msg1 = Message(Id("msg01"), Instant.now(), Id("user1"), Id("user2"), "Some message", false)
    val msg2 = Message(Id("msg02"), Instant.now(), Id("user2"), Id("user1"), "I like Burger King", false)
    val msg3 = Message(Id("msg03"), Instant.now(), Id("user1"), Id("user1"), "I prefer KFC", false)

    val producer = KafkaMultiProducer()

    producer.send[Post](post)

    producer.send[User](user1)
    producer.send[User](user2)

    producer.send[Message](msg1)
    producer.send[Message](msg2)
    producer.send[Message](msg3)

    println("All these messages were produced:")
    println(write(user1))
    println(write(user2))
    println(write(post))
    println(write(msg1))
    println(write(msg2))
    println(write(msg3))
  }
}
