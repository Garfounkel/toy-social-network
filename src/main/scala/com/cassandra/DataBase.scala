package com.cassandra

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import com.datastax.spark.connector._
import com.datastax.spark.connector.cql._

import java.net.URI

import java.time.Instant

import com.core._

object CassandraDB {

  def createDB() = {
    val conf = new SparkConf(true)
        .set("spark.cassandra.connection.host", "localhost")

    val sc = new SparkContext("local", "cassandra", conf)
    sc.setLogLevel("ERROR")

    // creating the database
    CassandraConnector(conf).withSessionDo { session =>
      // create keyspace here instead later on ..
      session.execute("DROP KEYSPACE IF EXISTS socialNetwork")
      session.execute("CREATE KEYSPACE socialNetwork WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1 }")

      session.execute("USE socialNetwork")

      // create Table users
      session.execute("DROP TABLE IF EXISTS users")
      session.execute("CREATE TABLE users(id TIMEUUID, updatedOn date, image text, username text, deleted boolean, PRIMARY KEY(username));")

      // inserting examples for table users
      session.execute("INSERT INTO users(id, updatedOn, image, username, deleted) VALUES (now(), toDate(now()), 'http://i.prntscr.com/XXS-8L2tR7id1MSgJDywoQ.png', 'Garfounkel', false)")
      session.execute("INSERT INTO users(id, updatedOn, image, username, deleted) VALUES (now(), toDate(now()), '', 'barthiex', false)")
      session.execute("INSERT INTO users(id, updatedOn, image, username, deleted) VALUES (now(), toDate(now()), 'http://i.prntscr.com/XXS-8L2tR7id1MSgJDywoQ.png', 'Simon', false)")
      session.execute("INSERT INTO users(id, updatedOn, image, username, deleted) VALUES (now(), toDate(now()), '', 'Karim', false)")
      session.execute("INSERT INTO users(id, updatedOn, image, username, deleted) VALUES (now(), toDate(now()), '', 'Nicolas', false)")

      session.execute("DROP TABLE IF EXISTS posts")
      session.execute("CREATE TABLE posts(id TIMEUUID, updatedOn date, author TIMEUUID, text text, image text, deleted boolean, PRIMARY KEY (id));")

      val user0 = session.execute("SELECT * FROM users WHERE username = 'Garfounkel'")
      if (!user0.isExhausted)
      {
        // inserting examples for table posts
        session.execute("INSERT INTO posts(id, updatedOn, author, text, image, deleted) VALUES (now(), toDate(now()), " + user0.one.getUUID("id") + " ,'Some Text', 'http://i.prntscr.com/XXS-8L2tR7id1MSgJDywoQ.png', false)")
      }

      session.execute("DROP TABLE IF EXISTS comments")
      session.execute("CREATE TABLE comments(id TIMEUUID, updatedOn date, postID TIMEUUID, author TIMEUUID, text text, deleted boolean, PRIMARY KEY (id));")

      // because user0.one exhausted user0, since there is only one element
      val user1 = session.execute("SELECT * FROM users WHERE username = 'Garfounkel'")
      if (!user1.isExhausted)
      {
        val authorID = user1.one.getUUID("id")
        val post0 = session.execute("SELECT * FROM posts WHERE author = " + authorID + "ALLOW FILTERING")
        if (!post0.isExhausted)
        {
          // inserting examples for table posts
          session.execute("INSERT INTO comments(id, updatedOn, postID, author, text, deleted) VALUES (now(), toDate(now()), " + authorID + ", " + post0.one.getUUID("id") + " ,'Some Text', false)")
        }
      }
    }

    sc.stop()
  }

  // ToDo: do not return null, use something better
  def findUser(username: String) : User = {
    val conf = new SparkConf(true)
        .set("spark.cassandra.connection.host", "localhost")

    val sc = new SparkContext("local", "cassandra", conf)
    sc.setLogLevel("ERROR")

    CassandraConnector(conf).withSessionDo{ session =>
      session.execute("USE socialNetwork")

      println("Looking for username " + username)

      val res = session.execute("SELECT * FROM users WHERE username = \'" + username + "\'").one

      sc.stop()

      if (res != null) {
        // ToDo: convert updatedOn to java.Instant
        User(Id[User](res.getUUID("id").toString()), Instant.now(), URI.create(res.getString("image")), res.getString("username"), res.getBool("deleted"))
      }
      else {
        null
      }
    }
  }

  def addUser(user: User) : Boolean = {
    val conf = new SparkConf(true)
        .set("spark.cassandra.connection.host", "localhost")

    val sc = new SparkContext("local", "cassandra", conf)
    sc.setLogLevel("ERROR")

    CassandraConnector(conf).withSessionDo{ session =>
      val req = session.execute("SELECT * FROM users WHERE username = \'" + user.username + "\'").one
      sc.stop
      session.execute("INSERT INTO users(id, updatedOn, image, username, deleted) VALUES (now(), toDate(now()), \'"
                                         + user.image.toString + "\', \'"
                                         + user.username + "\', " + user.deleted + " )").wasApplied
    }
  }

}
