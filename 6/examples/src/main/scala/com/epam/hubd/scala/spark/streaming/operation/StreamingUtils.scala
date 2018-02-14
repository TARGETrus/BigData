package com.epam.hubd.scala.spark.streaming.operation

import org.apache.spark.rdd.RDD
import org.joda.time.format.DateTimeFormat
import twitter4j.Status

import scala.io.Source

object StreamingUtils {

  val YYYYMMDDHHMM = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm")

  def loadTwitterCredentials = {
    Source.fromURL(getClass.getClassLoader.getResource("twitter.properties"))
      .getLines
      .map(_.split("="))
      .foreach(p => System.setProperty(p(0), p(1)))
  }

  def getTags(tweet: Status) = {
    tweet.getText.split("\\s").filter(_.startsWith("#")).map(_.toLowerCase.replaceAll("[.,]", ""))
  }

  def printRDD[T](rdd: RDD[T]) = {
    println(s"\nFetched ${rdd.count} tweets (limit 10")
    rdd.take(10)
      .foreach(println)
    //    Thread.sleep(800)
  }
}
