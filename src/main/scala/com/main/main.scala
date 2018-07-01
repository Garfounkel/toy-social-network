package com.main

import com.core._
import java.time.Instant
import java.net.URI


object Main {
  def main(args: Array[String]) {
    println("------ Main ------\n")

    val post = Post(Id[Post]("post0"), Instant.now(), Id[User]("user0"), "Some Text", URI.create("https://some-uri.com)"), false)
    println(post)

    val user = User(Id[User]("user0"), Instant.now(), URI.create("https://img)"), "Garfounkel", false)
    println(user)

    val comment = Comment(Id[Comment]("com0"), Id[Post]("post0"), Instant.now(), Id[User]("user0"), "Some Text", false)
    println(comment)

    println("\n------ Exit ------")
  }
}
