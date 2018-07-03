package com.cassandra

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import com.datastax.spark.connector._
import com.datastax.spark.connector.cql._

object CassandraDB {
  def createDB() = {
    val conf = new SparkConf(true)
        .set("spark.cassandra.connection.host", "localhost")

    val sc = new SparkContext("local", "test", conf)
    //sc.setLogLevel("ERROR")

    // meaning the cassandra table used is test.words
    val keyspace = "test"
    val table = "users"

    // creating the database
    CassandraConnector(conf).withSessionDo { session =>
      // create keyspace here instead later on ..
      session.execute("DROP KEYSPACE IF EXISTS socialNetwork")
      session.execute("CREATE KEYSPACE socialNetwork WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1 }")

      session.execute("USE socialNetwork")

      // create Table users
      session.execute("DROP TABLE IF EXISTS users")
      session.execute("CREATE TABLE users(id TIMEUUID, updatedOn date, image text, username text, deleted boolean, PRIMARY KEY(username, id));")

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
}
