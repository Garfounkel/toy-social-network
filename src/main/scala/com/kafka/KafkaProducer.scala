package com.kafka

import com.core._
import com.utils._

import java.util.Properties
import scala.concurrent._
import ExecutionContext.Implicits.global

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord, ProducerConfig, RecordMetadata}

import org.json4s._
import org.json4s.native.Serialization
import org.json4s.native.Serialization.{read, write}

trait Record[K, V] {
  def topic: String
  def key(value: V): K
  def timestamp(value: V): Long
}

object Producer {
  def apply[V] = new ProducerBuilder[V]
  class ProducerBuilder[V] {
    def apply[K](config: Properties)(implicit record: Record[K, V],
                                     keySerializer: write[K],
                                     valueSerializer: read[V]): KafkaProducer[K, V] =
      new KafkaProducer(config, keySerializer, valueSerializer)
  }

  implicit class KafkaProducerOps[K, V](kafkaProducer: KafkaProducer[K, V]) {
    def send(value: V)(implicit record: Record[K, V]): Future[RecordMetadata] = Future {
      kafkaProducer.send(new ProducerRecord(record.topic,
                                            null,
                                            record.timestamp(value),
                                            record.key(value),
                                            value)).get()
    }
  }
}
