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

  def StringToMessage(str: String): Message = read[Message](str)

  def LoadMessages(): RDD[Message] = {
    sc.textFile(pathToFile).flatMap(StringToMessage)
  }

  def SearchMessages(query: String): RDD[String] = {
    LoadMessages.filter(_.text contains query)
  }
}
