package com.core

import java.time.Instant
import java.net.URI
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


case class Post(id: Id[Post],
                updatedOn: Instant,
                author: Id[User],
                text: String,
                image: URI,
                deleted: Boolean)

object Post {
  implicit val topic: Topic[Post] = new Topic[Post] {
    val value = "posts"
  }
}
