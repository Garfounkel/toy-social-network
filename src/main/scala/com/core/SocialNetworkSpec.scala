package com.core

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD

object SocialNetwork {

  val pathToFile = "data/wordcount.txt"

  /**
   *  Load the data from the text file and return an RDD of words
   */
  def loadData(): RDD[String] = {
    val conf = new SparkConf()
                        .setAppName("Wordcount")
                        .setMaster("local[*]")

    val sc = SparkContext.getOrCreate(conf)

    sc.textFile(pathToFile)
      .flatMap(_.split(" "))
  }

  /**
   *  Now count how much each word appears!
   */
  def wordcount(): RDD[(String, Int)] = {
    val words = loadData()
    words.map(word => (word, 1)).reduceByKey{case (x, y) => x + y}
  }
}
