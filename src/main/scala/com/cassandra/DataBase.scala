package com.cassandra

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import com.datastax.spark.connector._
import com.datastax.spark.connector.cql._

object CassandraDB {
  /*def existDB(name : String) : Boolean = {
    CassandraConnector(conf).withSessionDo { session =>
      session.execute("SELECT * FROM " + name )
      //session.execute("CREATE KEYSPACE test2 WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': 1 }")
      //session.execute("CREATE TABLE test2.words (word text PRIMARY KEY, count int)")
    }
  }*/

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
      session.execute("CREATE TABLE users(id int PRIMARY KEY, updatedOn date, image text, username text, deleted boolean);")

      // inserting examples
      session.execute("INSERT INTO users(id, updatedOn, image, username, deleted) VALUES (1000, '2017-05-05', '', 'barthiex', false)")
      session.execute("INSERT INTO users(id, updatedOn, image, username, deleted) VALUES (1, '2017-05-05', 'http://i.prntscr.com/XXS-8L2tR7id1MSgJDywoQ.png', 'Simon', false)")
      session.execute("INSERT INTO users(id, updatedOn, image, username, deleted) VALUES (2, '2017-05-05', '', 'Karim', false)")
      session.execute("INSERT INTO users(id, updatedOn, image, username, deleted) VALUES (3, '2017-05-05', '', 'Nicolas', false)")
      /*session.execute("USE socialNetwork")
      session.execute("SELECT * FROM users")*/
    }

    // write two rows to the table:
    // val col = sc.parallelize(Seq(("Simon", 1), ("Karim", 2), ("Nico", 3), ("Berthier", 1000)))
    // col.saveAsCassandraTable(keyspace, table)

    //Read the table and print its contents:

    sc.stop()
  }
}
