package com.core

import java.time.Instant
import java.net.URI

case class Comment(id: Id[Comment],
                   postId: Id[Post],
                   updatedOn: Instant,
                   author: Id[User],
                   text: String,
                   deleted: Boolean)
