package com.core

import java.time.Instant
import java.net.URI

case class Post(id: Id[Post],
                updatedOn: Instant,
                author: Id[User],
                text: String,
                image: URI,
                deleted: Boolean)
