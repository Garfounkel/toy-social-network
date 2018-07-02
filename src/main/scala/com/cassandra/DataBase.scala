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
        .set("spark.cassandra.connection.host", "127.0.0.1")

    val sc = new SparkContext("spark://127.0.0.1:7077", "test", conf)
    //sc.setLogLevel("ERROR")

    // meaning the cassandra table used is test.words
    val keyspace = "test"
    val table = "users"

    CassandraConnector(conf).withSessionDo { session =>
      // create keyspace here instead later on ..
      session.execute("USE test")
      //session.execute("SELECT * FROM users" )
    }

    // write two rows to the table:
    // val col = sc.parallelize(Seq(("Simon", 1), ("Karim", 2), ("Nico", 3), ("Berthier", 1000)))
    // col.saveAsCassandraTable(keyspace, table)

    //Read the table and print its contents:
    val rdd = sc.cassandraTable(keyspace, table)
    rdd.foreach(println)

    sc.stop()
  }
}
