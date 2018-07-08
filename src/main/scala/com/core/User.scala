package com.core

import java.time.Instant
import java.net.URI
import com.kafka._
import com.utils._


case class User(id: Id[User],
                updatedOn: Instant,
                image: URI,
                deleted: Boolean)

object User {
  implicit val topic: Topic[User] = new Topic[User] {
    val value = "users"
  }
}
