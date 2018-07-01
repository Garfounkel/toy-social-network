package com.core

import java.time.Instant
import java.net.URI

case class User(id: Id[User],
                updatedOn: Instant,
                image: URI,
                username: String,
                deleted: Boolean)
