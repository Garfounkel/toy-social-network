package com.kafka

import com.utils._
import com.cassandra._

import java.util.concurrent._
import java.util.{Collections, Properties}

import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.clients.consumer.{ConsumerConfig, KafkaConsumer}
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.kafka.common.errors.WakeupException

import scala.collection.JavaConversions._

import org.json4s._
import org.json4s.native.Serialization
import org.json4s.native.Serialization.{read, write}

import java.time.Instant
import java.time.format.DateTimeFormatter
import com.github.kardapoltsev.json4s.javatime.{InstantSerializer}

class ConsumerExecutor[V](val brokers: String,
                          val groupId: String)
                         (implicit topic: Topic[V], m: Manifest[V]) {

  val props = createConsumerConfig(brokers, groupId)
  val consumer = new KafkaConsumer[String, String](props)
  var executor: ExecutorService = null

  def shutdown() = {
    if (consumer != null)
      consumer.wakeup();
  }

  def createConsumerConfig(brokers: String, groupId: String): Properties = {
    val props = new Properties()
    props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers)
    props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId)
    props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true")
    props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000")
    props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "30000")
    props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, classOf[StringDeserializer])
    props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, classOf[StringDeserializer])
    props
  }

  implicit val formats = Serialization.formats(NoTypeHints) +
                         InstantSerializer +
                         UriSerializer
  def run() = {
    try {
      consumer.subscribe(Collections.singletonList(this.topic.value))

      Executors.newSingleThreadExecutor.execute(new Runnable {
        override def run(): Unit = {
          while (true) {
            val records = consumer.poll(1000)

            for (record <- records) {
              println("Received message: (" +
                       record.key() + ", " +
                       record.value() +
                       ") at offset " + record.offset())
              val obj = read[V](record.value)
              println(obj)
              CassandraDB.add(obj)
            }
          }
        }
      })
    } catch {
      // Ignore exception because we are closing
      case _: WakeupException =>
    } finally {
      // consumer.close()
      if (executor != null)
        executor.shutdown()
    }
  }
}
