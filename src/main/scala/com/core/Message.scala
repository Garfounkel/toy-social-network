package com.core

import java.time.Instant
import com.kafka._
import com.utils._

import org.json4s._
import org.json4s.native.Serialization
import org.json4s.native.Serialization.{read, write}
import java.time.format.DateTimeFormatter
import com.github.kardapoltsev.json4s.javatime.{InstantSerializer}
import org.apache.kafka.common.serialization.{Deserializer, Serializer}
import java.util
import java.nio.charset._


case class Message(id: Id[Message],
                updatedOn: Instant,
                from: Id[User],
                to: Id[User],
                text: String,
                deleted: Boolean)

object Message {
  implicit val topic: Topic[Message] = new Topic[Message] {
    val value = "messages"
  }
}
