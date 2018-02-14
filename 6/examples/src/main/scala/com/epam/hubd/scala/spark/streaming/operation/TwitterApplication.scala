package com.epam.hubd.scala.spark.streaming.operation

import org.apache.spark.streaming._
import org.apache.spark.streaming.twitter.TwitterUtils
import org.apache.spark.{SparkConf, SparkContext}

object TwitterApplication {

  def main(args: Array[String]): Unit = {
    val sc = new SparkContext(new SparkConf()
      .setMaster("local[4]")
      .setAppName("spark-streaming-operations-scala")
      .set("spark.streaming.stopGracefullyOnShutdown","true"))

    // StreamingContext with 1 second batchWindow
    val ssc = new StreamingContext(sc, Seconds(1))
    // Checkpoint for stateful functions
    ssc.checkpoint("/home/ferenc_kis/spark_streaming/checkpoint")

    // Create Twitter Input DStream with filters
    StreamingUtils.loadTwitterCredentials
    val filters = Array("ios", "apple", "iphone", "android", "google", "nexus")
    val tweets = TwitterUtils.createStream(ssc, None, filters)

    // **********************
    // * DStream operations *
    // **********************

    // Filter only en language and get tweet texts
    // HashTags are not case sensitives
    val englishTweets = tweets.filter(_.getLang equals "en")
//    englishTweets.map(_.getText)
//      .print

    // Get Tags from tweet text and get separate tags with flatmap
    val tags = englishTweets.flatMap(tweet => StreamingUtils.getTags(tweet))
//    tags.print

    // Save as textfiles - partitions
//    tags.saveAsTextFiles("/home/ferenc_kis/spark_streaming/tags/microbatch")

    // Tagcount in microBatch
//    tags.map(tag => (tag,1))
//      .reduceByKey(_ + _)
//      .print()

    // ********************
    // *    ForeachRDD    *
    // ********************

    // ForeachRDD
//    tags.foreachRDD(rdd => {
//      rdd.distinct.saveAsTextFile("/home/ferenc_kis/spark_streaming/tags/foreachrdd")
//    })

    // ********************
    // *    Transform     *
    // ********************

    // Transform - applying distinct and sortBy - both unavailable in Streaming API
//    tags.transform(rdd => rdd.distinct.sortBy(tag => tag))
//      .print

    // ********************
    // * Join operations  *
    // ********************

    // Country Code RDD For Join
//    val countryCodeRDD = sc.textFile("/home/ferenc_kis/spark_streaming/language_codes")
//      .map(row => {
//        val split = row.split(",")
//        (split(0).toLowerCase, split(1))
//      })
//
//    // Stream - RDD Join
//    tweets.map(tweet => (tweet.getLang, tweet.getUser.getName))
//      .filter(!_._2.equals(""))
//      .transform(rdd => rdd.distinct
//        .join(countryCodeRDD)
//        .map(record => (record._2._1, record._2._2))
//      )
//      .print()

    // Stream - Stream Join
    val twitterUsers = englishTweets.map(tweet =>
      TwitterUser(tweet.getUser.getName, tweet.getUser.getLang, tweet.getUser.getFriendsCount, StreamingUtils.YYYYMMDDHHMM.print(tweet.getUser.getCreatedAt.getTime)))

    val tweetsWithUsers = englishTweets.flatMap(tweet => StreamingUtils.getTags(tweet).map((tweet.getUser.getName, _)))

//    twitterUsers.map(user => (user.name, user))
//      .join(tweetsWithUsers)
//      .map(record => (record._2._1.name, record._2._1.createdAt, record._2._1.friends, record._2._2))
//      .print

    // ********************
    // * Window functions *
    // ********************

    // Window - different window durations
//    tags.window(Seconds(1))
//      .foreachRDD((rdd, time) => {
//        println(s"CurrentTime=${time}")
//        println(s"FirstRDD=${rdd.count}")
//      })
//    tags.window(Seconds(2))
//      .foreachRDD(rdd => {
//        println(s"SecondRDD=${rdd.count}")
//        println("--------------------------")
//      })

    // countByWindow - tumbling (non overlapping) and sliding (overlapping)
//    englishTweets.countByWindow(Seconds(2), Seconds(1))
//      .foreachRDD(rdd => {
//        println(s"Sliding=${rdd.collect.mkString}")
//      })
//
//    englishTweets.countByWindow(Seconds(2), Seconds(2))
//      .foreachRDD(rdd => {
//        println(s"Tumbling=${rdd.collect.mkString}")
//        println("--------------------------")
//      })

    // reduceByKey vs reduceByKeyAndWindow
    val textsWithCountByLang = tweets
      .filter(tweet => List("en", "fr", "es") contains tweet.getLang)
      .map(status => (status.getLang, (status.getText.length, 1)))

//    textsWithCountByLang.reduceByKey((a, b) => (a._1 + b._1, a._2 + b._2))
//      .mapValues{ case (sumTextLengths, sumTweets) => sumTextLengths / sumTweets}
//      .foreachRDD((rdd, time) => {
//        println(s"CurrentTime=${time}")
//        println(s"Single=${rdd.collect.sortBy(_._1).mkString(" ")}")
//      })
//
//    textsWithCountByLang.reduceByKeyAndWindow((a, b) => (a._1 + b._1, a._2 + b._2), Seconds(10))
//      .mapValues{ case (sumTextLengths, sumTweets) => sumTextLengths / sumTweets}
//      .foreachRDD(rdd => {
//        println(s"Windowed=${rdd.collect.sortBy(_._1).mkString(" ")}")
//        println("--------------------------")
//      })

    // countByValueAndWindow
//    textsWithCountByLang.map(tweet => tweet._1)
//      .countByWindow(Seconds(5), Seconds(1))
//      .foreachRDD((rdd, time) => {
//        println(s"CurrentTime=${time}")
//        println(s"countByWindow=${rdd.collect.mkString(" ")}")
//      })
//
//    textsWithCountByLang.map(tweet => tweet._1)
//      .countByValueAndWindow(Seconds(5), Seconds(1))
//      .foreachRDD(rdd => {
//        println(s"countByValueAndWindow=${rdd.collect.mkString(" ")}")
//        println("--------------------------")
//      })

    // **********************
    // * Stateful functions *
    // **********************

    // updateStateByKey vs mapWithState - counting tweets by languages
    def updateFunction(newValues: Seq[Int], runningCount: Option[Int]): Option[Int] = {
      val newCount = runningCount.getOrElse(0) + newValues.foldLeft(0)(_ + _)
      Some(newCount)
    }

//    textsWithCountByLang.map(tweet => (tweet._1, 1))
//      .updateStateByKey(updateFunction)
//      .foreachRDD((rdd, time) => {
//        println(s"CurrentTime=${time}")
//        println(s"updateStateByKey=${rdd.collect.mkString(" ")}")
//      })
//
//
//    val updateCount = (key: String, value: Option[(Int, Int)], state: State[Long]) => {
//      val currentCount = value match {
//        case Some((_, count)) => count
//        case _ => 0
//      }
//      val newCount = currentCount + state.getOption.getOrElse(0L)
//      state.update(newCount)
//      (key, newCount)
//    }
//
//    textsWithCountByLang.mapWithState(StateSpec.function(updateCount))
//      .reduceByKey{ case (a,b) => Math.max(a, b) }
//      .foreachRDD(rdd => {
//        println(s"mapWithState=${rdd.collect.mkString(" ")}")
//        println("--------------------------")
//      })

    // mapWithState - tweet sentiment
    val updateSentiment = (batchTime: Time, key: String, value: Option[Int], state: State[TweetSentiment]) => {
      val currentValue = value.getOrElse(0)
      val sentiment = state.getOption().getOrElse(TweetSentiment(Seq(), "")).next(batchTime.milliseconds, currentValue)
      state.update(sentiment)
      Some(key, currentValue, sentiment.sentiment)
    }

    tags.map((_, 1))
      .reduceByKeyAndWindow(_ + _, Seconds(10))
      .mapWithState(StateSpec.function(updateSentiment))
      .foreachRDD(rdd => {
        val ordering = Ordering.by((r: (String, Int, String)) => r._2)
        println(rdd.takeOrdered(10)(ordering.reverse).mkString("\n"))
        println("--------------------------------")
      })

    ssc.start()
    ssc.awaitTermination()
  }
}
