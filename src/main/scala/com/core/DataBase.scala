package com.core

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import com.datastax.spark.connector._

object CassandraDB {
  def createDB() = {
    val conf = new SparkConf()
                          .setAppName("Wordcount")
                          .setMaster("local[*]")

    val sc = SparkContext.getOrCreate(conf)
    sc.setLogLevel("ERROR")

    // meaning the cassandra table used is test.words
    val keyspace = "test"
    val table = "users"

    // write two rows to the table:
    // val col = sc.parallelize(Seq(("Simon", 1), ("Karim", 2), ("Nico", 3), ("Berthier", 1000)))
    // col.saveAsCassandraTable(keyspace, table)

    //Read the table and print its contents:
    val rdd = sc.cassandraTable(keyspace, table)
    rdd.foreach(println)

    sc.stop()
  }

}
