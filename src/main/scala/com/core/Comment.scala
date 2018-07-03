package com.core

import java.time.Instant
import java.net.URI
import com.kafka._

case class Comment(id: Id[Comment],
                   postId: Id[Post],
                   updatedOn: Instant,
                   author: Id[User],
                   text: String,
                   deleted: Boolean)

object Comment {
 implicit val topic: Topic[Comment] = new Topic[Comment] {
   val value = "comments"
 }
}
