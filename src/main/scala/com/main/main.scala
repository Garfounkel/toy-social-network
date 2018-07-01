package com.main

import com.core._
import com.utils._

import java.net.URI

import org.json4s._
import org.json4s.native.Serialization
import org.json4s.native.Serialization.{read, write}

import java.time.Instant
import java.time.format.DateTimeFormatter
import com.github.kardapoltsev.json4s.javatime.{InstantSerializer}


object Main {
  implicit val formats = Serialization.formats(NoTypeHints) +
                         InstantSerializer +
                         UriSerializer


  def main(args: Array[String]) {
    println("------ Main ------\n")

    val uri = URI.create("http://i.prntscr.com/XXS-8L2tR7id1MSgJDywoQ.png")

    val post = Post(Id[Post]("post0"), Instant.now(), Id[User]("user0"), "Some Text", uri, false)
    println(post)

    val user = User(Id[User]("user0"), Instant.now(), uri, "Garfounkel", false)
    println(user)

    val comment = Comment(Id[Comment]("com0"), Id[Post]("post0"), Instant.now(), Id[User]("user0"), "Some Text", false)
    println(comment)

    val ser = write(post)
    val post2 = read[Post](ser)
    println(post2)

    println("\n------ Exit ------")
  }
}
