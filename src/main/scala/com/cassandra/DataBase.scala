package com.cassandra

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import com.datastax.spark.connector._
import com.datastax.spark.connector.cql._

import scala.collection.JavaConverters._

import java.net.URI

import java.time.Instant

import com.core._

object CassandraDB {

  val conf = new SparkConf(true)
      .set("spark.cassandra.connection.host", "localhost")
      .set("spark.driver.allowMultipleContexts", "true")

  /*val sc = new SparkContext("local", "cassandra", conf)
  sc.setLogLevel("ERROR")*/

  def createDB() = {
    // creating the database
    CassandraConnector(conf).withSessionDo { session =>

      // create keyspace here instead later on ..
      session.execute("DROP KEYSPACE IF EXISTS socialNetwork")
      session.execute("CREATE KEYSPACE socialNetwork WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1 }")

      session.execute("USE socialNetwork")

      /* USERS */
      session.execute("DROP TABLE IF EXISTS users")
      session.execute("CREATE TABLE users(id text, updatedOn text, image text, deleted boolean, PRIMARY KEY(id));")

      /* MESSAGES */
      session.execute("DROP TABLE IF EXISTS messages")
      session.execute("CREATE TABLE messages(id TIMEUUID, updatedOn text, author text, dest text, text text, deleted boolean, PRIMARY KEY(id));")

      /* POSTS */
      session.execute("DROP TABLE IF EXISTS posts")
      session.execute("CREATE TABLE posts(id TIMEUUID, updatedOn text, author text, text text, image text, deleted boolean, PRIMARY KEY (id));")

      /* COMMENTS */
      session.execute("DROP TABLE IF EXISTS comments")
      session.execute("CREATE TABLE comments(id TIMEUUID, updatedOn text, postID text, author text, text text, deleted boolean, PRIMARY KEY (id));")
    }
  }

  def addExamples() = {
    // creating the database
    CassandraConnector(conf).withSessionDo { session =>
      val today = Instant.now()

      // inserting examples for table users
      session.execute("INSERT INTO users(id, updatedOn, image, deleted) VALUES ()'Garfounkel', \'" + today + "\', 'http://i.prntscr.com/XXS-8L2tR7id1MSgJDywoQ.png', false)")
      session.execute("INSERT INTO users(id, updatedOn, image, deleted) VALUES ('barthiex', \'" + today + "\', '', false)")
      session.execute("INSERT INTO users(id, updatedOn, image, deleted) VALUES ('Simon', \'" + today + "\', 'http://i.prntscr.com/XXS-8L2tR7id1MSgJDywoQ.png', false)")
      session.execute("INSERT INTO users(id, updatedOn, image, deleted) VALUES ('Karim', \'" + today + "\', '', false)")
      session.execute("INSERT INTO users(id, updatedOn, image, deleted) VALUES ('Nicolas', \'" + today + "\', '', false)")


      val user0 = session.execute("SELECT * FROM users WHERE id = 'Garfounkel'")
      if (!user0.isExhausted) {
        // inserting examples for table posts
        session.execute("INSERT INTO posts(id, updatedOn, author, text, image, deleted) VALUES (now(), \'" + today + "\', " + user0.one.getUUID("id") + " ,'Some Text', 'http://i.prntscr.com/XXS-8L2tR7id1MSgJDywoQ.png', false)")
      }


      // because user0.one exhausted user0, since there is only one element
      val user1 = session.execute("SELECT * FROM users WHERE id = 'Garfounkel'")
      if (!user1.isExhausted) {
        val authorID = user1.one.getUUID("id")
        val post0 = session.execute("SELECT * FROM posts WHERE author = " + authorID + "ALLOW FILTERING")
        if (!post0.isExhausted) {
          // inserting examples for table posts
          session.execute("INSERT INTO comments(id, updatedOn, postID, author, text, deleted) VALUES (now(), \'" + today + "\', " + authorID + ", " + post0.one.getUUID("id") + " ,'Some Text', false)")
        }
      }
    }
  }

  // ToDo: do not return null, use something better
  def findUser(id: String) : User = {
    CassandraConnector(conf).withSessionDo{ session =>
      session.execute("USE socialNetwork")

      val res = session.execute("SELECT * FROM users WHERE id = \'" + id + "\'").one

      if (res != null) {
        User(Id[User](res.getString("id")),
             Instant.parse(res.getString("updatedOn")),
             URI.create(res.getString("image")),
             res.getBool("deleted"))
      }
      else {
        null
      }
    }
  }

  def getMessages() : RDD[Message] = {
    val sc = new SparkContext("local", "cassandra", conf)
    sc.setLogLevel("ERROR")

    val req = CassandraConnector(conf).withSessionDo{ session =>
      session.execute("USE socialNetwork")
      session.execute("SELECT * FROM messages").all.asScala
    }

    val tmp = req.map(elt => {
      Message(Id[Message](""),
      Instant.parse(elt.getString("updatedOn")),
      Id[User](elt.getString("author")),
      Id[User](elt.getString("dest")),
      elt.getString("text"),
      elt.getBool("deleted"))
    })
    val ret = sc.parallelize(tmp)

    ret
  }

  def toHDFS() = {

    val sc = new SparkContext("local", "cassandra", conf)
    sc.setLogLevel("ERROR")

    sc.cassandraTable("socialnetwork", "users")
      .saveAsObjectFile("hdfs://localhost:9000/user/hdfs/socialNetwork/users");
    sc.cassandraTable("socialnetwork", "messages")
      .saveAsObjectFile("hdfs://localhost:9000/user/hdfs/socialNetwork/messages");
    sc.cassandraTable("socialnetwork", "posts")
      .saveAsObjectFile("hdfs://localhost:9000/user/hdfs/socialNetwork/posts");

    sc.stop
  }

  def addUser(user: User) : Boolean = {
    CassandraConnector(conf).withSessionDo{ session =>
      session.execute("USE socialNetwork")
      session.execute("INSERT INTO users(id, updatedOn, image, deleted) VALUES (\' " + user.id.value + "\', \'" + user.updatedOn.toString() + "\', \'"
                                         + user.image.toString + "\', " + user.deleted + " )").wasApplied
    }
  }

  def addPost(post : Post) : Boolean = {
      CassandraConnector(conf).withSessionDo{ session =>
        session.execute("USE socialNetwork")
        session.execute("INSERT INTO posts(id, updatedOn, author, text, image, deleted) VALUES (now(), \'"
                         + post.updatedOn.toString() + "\', \'" + post.author.value + "\', \'"
                         + post.text +  "\', \'" + post.image.toString + "\', " + post.deleted + ")").wasApplied
    }
  }

  def addMessage(message : Message) : Boolean = {
    CassandraConnector(conf).withSessionDo{ session =>
      session.execute("USE socialNetwork")
      session.execute("INSERT INTO messages(id, updatedOn, author, dest, text, deleted) VALUES (now(), \'"
                       + message.updatedOn.toString() + "\', \'" + message.author.value + "\', \'" +  message.dest.value + "\', \'"
                       + message.text +  "\', " + message.deleted + ")").wasApplied
    }
  }

  def add[T](obj : T) : Boolean = obj match {
    case u : User => addUser(u)
    case m : Message => addMessage(m)
    case p : Post => addPost(p)
  }

  //sc.stop
}
