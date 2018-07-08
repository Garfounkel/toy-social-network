package com.query

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
import java.time.temporal.ChronoUnit
import java.time.format.DateTimeFormatter
import com.github.kardapoltsev.json4s.javatime.{InstantSerializer}
import org.apache.kafka.common.errors.WakeupException

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import com.datastax.spark.connector._
import com.datastax.spark.connector.cql._


object Query {
  val conf = new SparkConf()
      .setAppName("Toy-social-network")
      .setMaster("local[*]")

  val sc = SparkContext.getOrCreate(conf)
  sc.setLogLevel("ERROR")

  val pathToFile = ""

  implicit val formats = Serialization.formats(NoTypeHints) +
                         InstantSerializer +
                         UriSerializer

  def LoadMessages(): RDD[Message] = {
    val db = sc.objectFile[CassandraRow]("hdfs://localhost:9000/user/hdfs/socialNetwork/messages")
    db.map(elt => {
        Message(Id[Message](""),
        Instant.parse(elt.getString("updatedon")),
        Id[User](elt.getString("author")),
        Id[User](elt.getString("dest")),
        elt.getString("text"),
        elt.getBoolean("deleted"))
    })
  }

  def SearchMessages(query: String): RDD[Message] = {
    LoadMessages.filter(_.text contains query)
  }

  def LoadPosts(): RDD[Post] = {
    sc.textFile(pathToFile).map(read[Post])
  }

  def SearchPosts(query: String): RDD[Post] = {
    LoadPosts.filter(_.text contains query)
  }

  def SearchMessagesSince(query: String, timevalue: Int, unit: ChronoUnit): RDD[Message] = {
    SearchMessages(query).filter(_.updatedOn isAfter Instant.now().minus(timevalue, unit))
  }
}
